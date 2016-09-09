package registry

import (
	"encoding/json"
	"fmt"
)

func marshal(obj interface{}) (string, error) {
	encoded, err := json.Marshal(obj)
	if err == nil {
		return string(encoded), nil
	}
	return "", fmt.Errorf("unable to JSON-serialize object: %s", err)
}

func unmarshal(val []byte, obj interface{}) error {
	err := json.Unmarshal(val, &obj)
	if err == nil {
		return nil
	}
	return fmt.Errorf("unable to JSON-deserialize object: %s", err)
}
