package starflow

import (
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// customProtoRegistry combines the global registry with custom proto files
type customProtoRegistry struct {
	globalRegistry *protoregistry.Files
	customFiles    map[string]protoreflect.FileDescriptor
}

func newCustomProtoRegistry() *customProtoRegistry {
	return &customProtoRegistry{
		globalRegistry: protoregistry.GlobalFiles,
		customFiles:    make(map[string]protoreflect.FileDescriptor),
	}
}

func (r *customProtoRegistry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	// Try custom files first
	for _, fd := range r.customFiles {
		if desc := fd.Messages().ByName(name.Name()); desc != nil {
			return desc, nil
		}
		if desc := fd.Enums().ByName(name.Name()); desc != nil {
			return desc, nil
		}
		if desc := fd.Services().ByName(name.Name()); desc != nil {
			return desc, nil
		}
	}
	// Then try global registry
	return r.globalRegistry.FindDescriptorByName(name)
}

func (r *customProtoRegistry) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if fd, ok := r.customFiles[path]; ok {
		return fd, nil
	}
	return r.globalRegistry.FindFileByPath(path)
}
