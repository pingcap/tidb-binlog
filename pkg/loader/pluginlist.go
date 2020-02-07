package loader

import (
	"errors"
	"fmt"
	"plugin"
	"sync"
)

const FactorFunc = "NewPlugin"

type PluginList struct {
	mp sync.Map
}

func (pl *PluginList) GetPluginList() sync.Map {
	return pl.mp
}

func (pl *PluginList) SetPlugin(name string, p Plugin) *PluginList {
	if len(name) == 0 || p == nil {
		return pl
	}
	pl.mp.Store(name, p)
	return pl
}

func (pl *PluginList) DeletePlugin(name string) {
	pl.mp.Delete(name)
}

func (lp *PluginList) GetAllPluginsName() []string {
	var ns []string = make([]string, 0)
	lp.mp.Range(func(k, v interface{}) bool {
		name, ok := k.(string)
		if !ok {
			return true
		}
		ns = append(ns, name)
		return true
	})
	return ns
}

func (lp *PluginList) loadPluginByName(path, name string) error {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return errors.New(fmt.Sprintf("Open %s failed, err: %s", fp, err.Error()))
	}
	f, err := p.Lookup(FactorFunc)
	if err != nil {
		return errors.New(fmt.Sprint("Lookup %s faled", FactorFunc))
	}
	newPlugin, ok := f.(func() Plugin)
	if !ok {
		return errors.New(fmt.Sprint("Type of %s is incorrect", FactorFunc))
	}
	pr := newPlugin()
	lp.SetPlugin(name, pr)
	return nil
}

func (lp *PluginList) LoadPluginByNames(path string, names []string) error {
	if path == "" || names == nil || len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if err := lp.loadPluginByName(path, name); err != nil {
			return err
		}
	}
	return nil
}
