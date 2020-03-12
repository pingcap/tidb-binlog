package plugin

import (
	"fmt"
	"plugin"
	"sync"
)

//Kind is the plugin's type thant we supported currently
type Kind uint8

const (
	//SyncerFilter is one kind of Plugin for syncer
	SyncerFilter Kind = iota
	//ExecutorExtend is one kind of Plugin for loader
	ExecutorExtend
	//LoaderInit is one kind of Plugin for loader
	LoaderInit
	//LoaderDestroy is one kind of Plugin for loader
	LoaderDestroy
	//FactorFunc is the factory of all plugins
	FactorFunc = "NewPlugin"
)

//EventHooks is a map of hook name to hook
type EventHooks struct {
	sync.Map
}

func (ehs *EventHooks) setPlugin(name string, plg interface{}) *EventHooks {
	if len(name) == 0 || ehs == nil {
		return ehs
	}
	ehs.Store(name, plg)
	return ehs
}

//GetAllPluginsName is get all names of plugin
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

//LoadPlugin can load plugin by plugin's name
func LoadPlugin(path, name string) (plugin.Symbol, error) {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("Open %s failed. err: %s", fp, err.Error())
	}

	return p.Lookup(FactorFunc)
}

//RegisterPlugin register plugin to EventHooks
func RegisterPlugin(ehs *EventHooks, name string, plg interface{}) {
	ehs.setPlugin(name, plg)
}
