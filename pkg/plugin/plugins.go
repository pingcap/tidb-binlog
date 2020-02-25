package plugin

import (
	"errors"
	"fmt"
	"plugin"
	"sync"
)

//Plugin type we supported currently
type Kind uint8
const (
	SyncerPlugin Kind = iota
	LoaderPlugin

	FactorFunc = "NewPlugin"
)

//EventHooks is a map of hook name to hook
type EventHooks struct {
	sync.Map
}

func (ehs *EventHooks) SetPlugin(name string, plg interface{}) *EventHooks {
	if len(name) == 0 || ehs == nil {
		return ehs
	}
	ehs.Store(name, plg)
	return ehs
}

func (ehs *EventHooks) GetAllPluginsName() []string {
	if ehs == nil {
		return nil
	}
	var ns []string = make([]string, 0)
	ehs.Range(func(k, v interface{}) bool {
		name, ok := k.(string)
		if !ok {
			return true
		}
		ns = append(ns, name)
		return true
	})
	return ns
}

//LoadPlugin() can load plugin by plugin's name
func LoadPlugin(eh *EventHooks, path, name string) (plugin.Symbol, error) {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Open %s failed, err: %s", fp, err.Error()))
	}

	return p.Lookup(FactorFunc)
}

//RegisterPlugin() register plugin to EventHooks
func RegisterPlugin(ehs *EventHooks, name string, plg interface{}) {
	ehs.SetPlugin(name, plg)
}