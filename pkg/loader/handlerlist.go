package loader

import (
	"errors"
	"fmt"
	"plugin"
	"sync"
)

const FactorFunc = "DoFilter"

type HandlerList struct {
	mp sync.Map
}

func (hl *HandlerList) GetHandlerList() sync.Map {
	return hl.mp
}

func (hl *HandlerList) SetHandler(name string, h Handler) *HandlerList {
	if len(name) == 0 || h == nil {
		return hl
	}
	hl.mp.Store(name, h)
	return hl
}

func (hl *HandlerList) DeleteHandler(name string) {
	hl.mp.Delete(name)
}

func (hl *HandlerList) GetAllHandlersName() []string {
	var ns []string = make([]string, 0)
	hl.mp.Range(func(k, v interface{}) bool {
		name, ok := k.(string)
		if !ok {
			return true
		}
		ns = append(ns, name)
		return true
	})
	return ns
}

func (hl *HandlerList) GetAllPlugins() *PluginList {
	pl := PluginList{}
	hl.mp.Range(func(k, v interface{}) bool {
		h, ok := v.(Handler)
		if !ok {
			return true
		}
		n, ok := k.(string)
		if !ok {
			return true
		}
		pl.SetPlugin(n, h.NewPlugin())
		return true
	})
	return &pl
}

func (hl *HandlerList) loadHandlerByName(path, name string) error {
	fp := path + "/" + name
	p, err := plugin.Open(fp)
	if err != nil {
		return errors.New(fmt.Sprintf("Open %s failed, err: %s", fp, err.Error()))
	}
	f, err := p.Lookup(FactorFunc)
	if err != nil {
		return errors.New(fmt.Sprint("Lookup %s faled", FactorFunc))
	}
	newHandler, ok := f.(func() Handler)
	if !ok {
		return errors.New(fmt.Sprint("Type of %s is incorrect", FactorFunc))
	}
	hr := newHandler()
	hl.SetHandler(name, hr)
	return nil
}

func (hl *HandlerList) LoadHandlerByNames(path string, names []string) error {
	if path == "" || names == nil || len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if err := hl.loadHandlerByName(path, name); err != nil {
			return err
		}
	}
	return nil
}
