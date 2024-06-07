/*
Copyright 2023 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vgpu

import (
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/go-gpuallocator/gpuallocator"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"volcano.sh/k8s-device-plugin/pkg/plugin/vgpu/config"
)

// NvidiaDevicePlugin implements the Kubernetes device plugin API
type NvidiaVGpuDevicePlugin struct {
	ResourceManager
	deviceCache      *DeviceCache
	resourceName     string
	deviceListEnvvar string
	allocatePolicy   gpuallocator.Policy
	socket           string

	server        *grpc.Server
	cachedDevices []*Device
	health        chan *Device
	stop          chan interface{}
	changed       chan struct{}
	migStrategy   string
}

// NewNvidiaVGpuDevicePlugin returns an initialized NvidiaDevicePlugin
func NewNvidiaVGpuDevicePlugin(resourceName string, deviceCache *DeviceCache, allocatePolicy gpuallocator.Policy, socket string) *NvidiaVGpuDevicePlugin {
	return &NvidiaVGpuDevicePlugin{
		deviceCache:    deviceCache,
		resourceName:   resourceName,
		allocatePolicy: allocatePolicy,
		socket:         socket,
		migStrategy:    "none",

		// These will be reinitialized every
		// time the plugin server is restarted.
		server: nil,
		health: nil,
		stop:   nil,
	}
}

// NewMIGNvidiaVGpuDevicePlugin returns an initialized NvidiaDevicePlugin
func NewMIGNvidiaVGpuDevicePlugin(resourceName string, resourceManager ResourceManager, deviceListEnvvar string, allocatePolicy gpuallocator.Policy, socket string) *NvidiaDevicePlugin {
	return &NvidiaDevicePlugin{
		ResourceManager:  resourceManager,
		resourceName:     resourceName,
		deviceListEnvvar: deviceListEnvvar,
		allocatePolicy:   allocatePolicy,
		socket:           socket,

		// These will be reinitialized every
		// time the plugin server is restarted.
		cachedDevices: nil,
		server:        nil,
		health:        nil,
		stop:          nil,
		migStrategy:   "mixed",
	}
}

func (m *NvidiaVGpuDevicePlugin) initialize() {
	var err error
	if strings.Compare(m.migStrategy, "mixed") == 0 {
		m.cachedDevices = m.ResourceManager.Devices()
	}
	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	m.health = make(chan *Device)
	m.stop = make(chan interface{})
	check(err)
}

func (m *NvidiaVGpuDevicePlugin) cleanup() {
	close(m.stop)
	m.server = nil
	m.health = nil
	m.stop = nil
}

// Name returns the name of the plugin
func (m *NvidiaVGpuDevicePlugin) Name() string {
	return "Volcano-vgpu-Plugin"
}

// Start starts the gRPC server, registers the device plugin with the Kubelet,
// and starts the device healthchecks.
func (m *NvidiaVGpuDevicePlugin) Start() error {
	m.initialize()

	err := m.Serve()
	if err != nil {
		log.Printf("Could not start device plugin for '%s': %s", m.resourceName, err)
		m.cleanup()
		return err
	}
	log.Printf("Starting to serve '%s' on %s", m.resourceName, m.socket)

	err = m.Register()
	if err != nil {
		log.Printf("Could not register device plugin: %s", err)
		m.Stop()
		return err
	}
	log.Printf("Registered device plugin for '%s' with Kubelet", m.resourceName)

	if strings.Compare(m.migStrategy, "none") == 0 {
		m.deviceCache.AddNotifyChannel("plugin", m.health)
	} else if strings.Compare(m.migStrategy, "mixed") == 0 {
		go m.CheckHealth(m.stop, m.cachedDevices, m.health)
	} else {
		log.Panicln("migstrategy not recognized", m.migStrategy)
	}
	return nil
}

// Stop stops the gRPC server.
func (m *NvidiaVGpuDevicePlugin) Stop() error {
	if m == nil || m.server == nil {
		return nil
	}
	log.Printf("Stopping to serve '%s' on %s", m.resourceName, m.socket)
	m.deviceCache.RemoveNotifyChannel("plugin")
	m.server.Stop()
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	m.cleanup()
	return nil
}

func (m *NvidiaVGpuDevicePlugin) DevicesNum() int {
	return len(m.apiDevices())
}

// Serve starts the gRPC server of the device plugin.
func (m *NvidiaVGpuDevicePlugin) Serve() error {
	os.Remove(m.socket)
	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	pluginapi.RegisterDevicePluginServer(m.server, m)

	go func() {
		lastCrashTime := time.Now()
		restartCount := 0
		for {
			log.Printf("Starting GRPC server for '%s'", m.resourceName)
			err := m.server.Serve(sock)
			if err == nil {
				break
			}

			log.Printf("GRPC server for '%s' crashed with error: %v", m.resourceName, err)

			// restart if it has not been too often
			// i.e. if server has crashed more than 5 times and it didn't last more than one hour each time
			if restartCount > 5 {
				// quit
				log.Fatalf("GRPC server for '%s' has repeatedly crashed recently. Quitting", m.resourceName)
			}
			timeSinceLastCrash := time.Since(lastCrashTime).Seconds()
			lastCrashTime = time.Now()
			if timeSinceLastCrash > 3600 {
				// it has been one hour since the last crash.. reset the count
				// to reflect on the frequency
				restartCount = 1
			} else {
				restartCount++
			}
		}
	}()

	// Wait for server to start by launching a blocking connexion
	conn, err := m.dial(m.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

// Register registers the device plugin for the given resourceName with Kubelet.
func (m *NvidiaVGpuDevicePlugin) Register() error {
	conn, err := m.dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(m.socket),
		ResourceName: m.resourceName,
		Options:      &pluginapi.DevicePluginOptions{},
	}

	_, err = client.Register(context.Background(), reqt)
	if err != nil {
		return err
	}
	return nil
}

// GetDevicePluginOptions returns the values of the optional settings for this plugin
func (m *NvidiaVGpuDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	options := &pluginapi.DevicePluginOptions{}
	return options, nil
}

// ListAndWatch lists devices and update that list according to the health status
func (m *NvidiaVGpuDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	_ = s.Send(&pluginapi.ListAndWatchResponse{Devices: m.apiDevices()})
	for {
		select {
		case <-m.stop:
			return nil
		case d := <-m.health:
			// FIXME: there is no way to recover from the Unhealthy state.
			//d.Health = pluginapi.Unhealthy
			log.Printf("'%s' device marked unhealthy: %s", m.resourceName, d.ID)
			_ = s.Send(&pluginapi.ListAndWatchResponse{Devices: m.apiDevices()})
		}
	}
}

func (m *NvidiaVGpuDevicePlugin) MIGAllocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	responses := pluginapi.AllocateResponse{}
	for _, req := range reqs.ContainerRequests {
		for _, id := range req.DevicesIDs {
			if !m.deviceExists(id) {
				return nil, fmt.Errorf("invalid allocation request for '%s': unknown device: %s", m.resourceName, id)
			}
		}

		response := pluginapi.ContainerAllocateResponse{}

		uuids := req.DevicesIDs
		deviceIDs := m.deviceIDsFromUUIDs(uuids)

		response.Envs = m.apiEnvs(m.deviceListEnvvar, deviceIDs)

		klog.Infof("response=", response.Envs)
		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}

	return &responses, nil
}

// Allocate which return list of devices.
func (m *NvidiaVGpuDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	klog.Infoln("Allocate vgpu", reqs.ContainerRequests)
	responses := pluginapi.AllocateResponse{}
	for _, containerAllocateRequest := range reqs.ContainerRequests {
		response := pluginapi.ContainerAllocateResponse{}
		response.Envs = make(map[string]string)
		var gpuDevicesIDs []string
		for _, vgpuDevicesID := range containerAllocateRequest.DevicesIDs {
			gpuDevicesID := vgpuDevicesID[0:strings.LastIndex(vgpuDevicesID, "-")]
			gpuDevicesIDs = append(gpuDevicesIDs, gpuDevicesID)
		}
		response.Envs["NVIDIA_VISIBLE_DEVICES"] = strings.Join(gpuDevicesIDs, ",")
		response.Envs["CUDA_DEVICE_MEMORY_SHARED_CACHE"] = fmt.Sprintf("/tmp/vgpu/%v.cache", uuid.NewUUID())

		os.MkdirAll("/tmp/vgpulock", 0777)
		os.Chmod("/tmp/vgpulock", 0777)
		hostHookPath := os.Getenv("HOOK_PATH")

		response.Mounts = append(response.Mounts,
			&pluginapi.Mount{ContainerPath: "/usr/local/vgpu/libvgpu.so",
				HostPath: hostHookPath + "/libvgpu.so",
				ReadOnly: true},
			&pluginapi.Mount{ContainerPath: "/tmp/vgpulock",
				HostPath: "/tmp/vgpulock",
				ReadOnly: false},
		)
		found := false
		if !found {
			response.Mounts = append(response.Mounts, &pluginapi.Mount{ContainerPath: "/etc/ld.so.preload",
				HostPath: hostHookPath + "/vgpu/ld.so.preload",
				ReadOnly: true},
			)
		}
		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}
	klog.Infoln("Allocate Response", responses.ContainerResponses)
	return &responses, nil
}

// PreStartContainer is unimplemented for this plugin
func (m *NvidiaVGpuDevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// dial establishes the gRPC communication with the registered device plugin.
func (m *NvidiaVGpuDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (m *NvidiaVGpuDevicePlugin) Devices() []*Device {
	if strings.Compare(m.migStrategy, "none") == 0 {
		return m.deviceCache.GetCache()
	}
	if strings.Compare(m.migStrategy, "mixed") == 0 {
		return m.ResourceManager.Devices()
	}
	log.Panic("migStrategy not recognized,exiting...")
	return []*Device{}
}

func (m *NvidiaVGpuDevicePlugin) deviceExists(id string) bool {
	for _, d := range m.cachedDevices {
		if d.ID == id {
			return true
		}
	}
	return false
}

func (m *NvidiaVGpuDevicePlugin) deviceIDsFromUUIDs(uuids []string) []string {
	return uuids
}

func (m *NvidiaVGpuDevicePlugin) apiDevices() []*pluginapi.Device {
	if strings.Compare(m.migStrategy, "mixed") == 0 {
		var pdevs []*pluginapi.Device
		for _, d := range m.cachedDevices {
			pdevs = append(pdevs, &d.Device)
		}
		return pdevs
	}
	devices := m.Devices()
	klog.Infof("物理gpu卡数量: %d", len(devices))

	// 计算gpu整卡数量，计算逻辑：总数-用于vgpu切分的数量
	gpuCount := len(devices) - int(config.DeviceSplitGpuCount)
	klog.Infof("用于gpu整卡调度数量: %d", gpuCount)
	klog.Infof("用于gpu共享调度数量: %d", config.DeviceSplitGpuCount)
	klog.Infof("一张gpu卡切分为: %d张vgpu", config.DeviceSplitCount)

	// 计算用于vgpu切分的gpu起始序号
	vgpuStartIdx := gpuCount
	if vgpuStartIdx < 0 {
		vgpuStartIdx = 0
	}
	klog.Infof("用于gpu共享调度的gpu起始序号: %d", vgpuStartIdx)
	var res []*pluginapi.Device
	for i := vgpuStartIdx; i < len(devices); i++ {
		dev := devices[i]
		klog.Infof("开始切分序号为: %d的gpu卡，gpu uuid为: %s", i, dev.ID)
		for j := uint(0); j < config.DeviceSplitCount; j++ {
			id := fmt.Sprintf("%v-%v", dev.ID, j)
			res = append(res, &pluginapi.Device{
				ID:       id,
				Health:   dev.Health,
				Topology: nil,
			})
		}
	}
	klog.Infof("用于gpu共享调度向kubelet注册的数据: %v", res)
	return res
}

func (m *NvidiaVGpuDevicePlugin) apiEnvs(envvar string, deviceIDs []string) map[string]string {
	return map[string]string{
		envvar: strings.Join(deviceIDs, ","),
	}
}
