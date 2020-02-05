package loader

type Handler interface {
	NewPlugin() Plugin
}
