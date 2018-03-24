package registry

import (
	"reflect"
	"sync"
)

var registry = struct {
	sync.Mutex
	componentInterfaces map[string]*componentInterface
}{
	componentInterfaces: make(map[string]*componentInterface),
}

type componentInterface struct {
	sync.Mutex
	iface      reflect.Type
	components map[string]interface{}
}

func NewComponentInterface(iface interface{}) *componentInterface {
	ep := &componentInterface{
		iface:      reflect.TypeOf(iface).Elem(),
		components: make(map[string]interface{}),
	}
	registry.Lock()
	defer registry.Unlock()

	prev, ok := registry.componentInterfaces[ep.iface.Name()]
	if !ok {
		registry.componentInterfaces[ep.iface.Name()] = ep
		return ep
	}
	return prev
}

func (ep *componentInterface) register(component interface{}, name string) bool {
	ep.Lock()
	defer ep.Unlock()
	if name == "" {
		name = reflect.TypeOf(component).Elem().Name()
	}
	_, exists := ep.components[name]
	if exists {
		return false
	}
	ep.components[name] = component
	return true
}

func (ep *componentInterface) lookup(name string) (ext interface{}, ok bool) {
	ep.Lock()
	defer ep.Unlock()
	ext, ok = ep.components[name]
	return
}

func Register(component interface{}, name string) []string {
	registry.Lock()
	defer registry.Unlock()
	var ifaces []string
	for _, iface := range implements(component) {
		if ok := registry.componentInterfaces[iface].register(component, name); ok {
			ifaces = append(ifaces, iface)
		}
	}
	return ifaces
}

func implements(component interface{}) []string {
	var ifaces []string
	for name, ep := range registry.componentInterfaces {
		if reflect.TypeOf(component).Implements(ep.iface) {
			ifaces = append(ifaces, name)
		}
	}
	return ifaces
}

func GetComponent(iface interface{}, name string) interface{} {
	ifName := reflect.TypeOf(iface).Elem().Name()
	ep := registry.componentInterfaces[ifName]
	if ep == nil {
		return nil
	}
	component, _ := ep.lookup(name)
	return component
}
