package builtin

import "bitbucket.org/primelogic_io/bitlantern/service/dataflow"

var (
	// This should be one of the rare cases where a package variable makes sense, since these are built-in functions.
	// There should really only be one instance of this function provider service.
	instance *FunctionProvider
)

// BuiltinFunctionProvider implements FunctionProvider for built-in (i.e. Go implemented) functions.
type FunctionProvider struct {
	functions []Function
}

type Function struct {
	FunctionSpec dataflow.FunctionSpec
	NewFunction  func() dataflow.Function
}

// DefaultInstance returns the default instance of the builtin.FunctionProvider. This instance should be used as it is
// the instance that all the builtin functions will be registering themselves
func DefaultInstance() *FunctionProvider {
	if instance == nil {
		instance = &(FunctionProvider{})
	}

	return instance
}

// AddBuiltinFunction adds a new BuiltinFunction to the BuiltinFunctionProvider
func (bfp *FunctionProvider) RegisterFunction(bf Function) error {
	bfp.functions = append(bfp.functions, bf)
	return nil
}

// GetFunctionSpecByKey delegates to the FunctionProvider instances in the AggregatingFunctionProvider. The first match is returned.
func (bfp *FunctionProvider) GetFunctionSpecByKey(key string) (*dataflow.FunctionSpec, error) {
	for i := range bfp.functions {
		cur := bfp.functions[i]
		if cur.FunctionSpec.Key == key {
			return &(cur.FunctionSpec), nil
		}
	}

	return nil, nil
}

// GetExecutableFunction finds, builds, and returns an executable Function whose key matches the one passed in.
func (bfp *FunctionProvider) NewFunction(key string) (dataflow.Function, error) {
	for i := range bfp.functions {
		cur := bfp.functions[i]
		if cur.FunctionSpec.Key == key {
			return cur.NewFunction(), nil
		}
	}

	return nil, nil
}
