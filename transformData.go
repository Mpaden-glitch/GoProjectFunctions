package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"bytes"
	"fmt"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"text/template"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "transformData",
				Name:          "Adds a field",
				Description:   "Adds a field via expression based on existing field",
				Category:      "Data",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(transformData{})
			},
		})
}

type transformData struct {
}

type transformDataJSConfig struct {
	// splits contains the rules for data splitting. The first rule that matches the data will be used.
	rules []transformDataConfig
}

type transformDataConfig struct {
	jsExpression string
	outputField  string
	jsFuncName   string
}

// buildConfig builds a splitConfig from the passed in map. The map must be in the form:
// {
//		"defaultOutputPort": "not poor",
//		"rules": [
//			{
//				"jsCondition": "gender.lowerCase().startsWith('f')",
//				"outputPort": "poor"
//			}
//		]
// }
//
// rules.value can either be a value literal (ex: 100.00, "M", etc.) or a reference to another field in the record.
// rules.valueType accepts the values: "literal", "field"
func (f *transformData) buildConfig(config map[string]interface{}) (transformDataJSConfig, error) {
	c := transformDataJSConfig{}

	rules := config["rules"].([]interface{})
	c.rules = make([]transformDataConfig, len(rules))

	for i := 0; i < len(rules); i++ {
		curRuleMap := rules[i].(map[string]interface{})
		newRule := transformDataConfig{}

		newRule.outputField = curRuleMap["outputField"].(string)
		newRule.jsExpression = curRuleMap["jsExpression"].(string)

		c.rules[i] = newRule
	}

	return c, nil
}

func (f *transformData) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	splitOpts, err := f.buildConfig(config)
	if err != nil {
		panic("Error transforming the data")
	}

	//Open the data stream
	reader := in.PortReader(dataflow.DEFAULT_INPUT_PORT_NAME)
	err = reader.Open()
	if err != nil {
		panic("Unable to read record from input")
	}

	jsDataHelper := `
		String.prototype.toTitleCase = function(txt) {
			return this.replace(/\w\S*/g, function(txt){return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();});
    	};
		
	`

	//JS Template - We will wrap the condition express in a function that can be called more easily
	jsFuncTemplate, err := template.New("someName").Parse(`
		var {{.FuncName}} = function(data) {
			return ({{.Expression}});
		};
	`)

	vm := otto.New()

	//Run helper JS
	vm.Run(jsDataHelper)

	//Parse all the JS rules
	for i := range splitOpts.rules {
		curRule := &(splitOpts.rules[i])

		var b bytes.Buffer
		templateVars := make(map[string]string)
		templateVars["Expression"] = curRule.jsExpression
		templateVars["FuncName"] = fmt.Sprintf("transformData_%d", i)
		jsFuncTemplate.Execute(&b, templateVars)

		fmt.Printf("\n\n%s", b.String())

		_, err := vm.Run(b.String())
		if err != nil {
			panic(err)
		}

		curRule.jsFuncName = templateVars["FuncName"]
	}

	//Loop through all data
	for reader.HasNext() {
		rec, err := reader.Next()
		if err != nil {
			panic("Failed to read record")
		}

		//We should perform a check here to make sure we are getting records instead of files
		recVal, err := rec.GetAsRecord()
		if err != nil {
			panic("Failed to read record as a map")
		}

		/* START - The actual work */

		// Run the script rules for each record
		for i := range splitOpts.rules {
			curRule := splitOpts.rules[i]
			recData, _ := rec.GetAsRecord()
			value, err := vm.Call(curRule.jsFuncName, nil, recData)
			if err != nil {
				panic(err)
			}

			newVal, err := value.Export()
			if err != nil {
				panic(err)
			}

			recVal.Set(curRule.outputField, newVal)
		}
		out.WriteRecord(dataflow.DEFAULT_OUTPUT_PORT_NAME, &recVal)
	}
	/* END - The actual work */

	return nil
}
