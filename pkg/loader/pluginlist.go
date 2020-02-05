package loader

import "sync"

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
