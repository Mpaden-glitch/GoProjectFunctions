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
				Key:           "splitOnFieldJS",
				Name:          "Split Data by Field - JS",
				Description:   "Splits the input data using Javascript for conditions/rules",
				Category:      "Data",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(splitOnFieldJS{})
			},
		})
}

type splitOnFieldJS struct {
}

type splitConfigJS struct {
	// splits contains the rules for data splitting. The first rule that matches the data will be used.
	rules []splitRuleJS

	// defaultOutputPort specifies the output port into which to send data that doesn't match any other split rule
	defaultOutputPort string
}

type splitRuleJS struct {
	jsCondition string
	outputPort  string
	jsFuncName  string
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
func (f *splitOnFieldJS) buildConfig(config map[string]interface{}) (splitConfigJS, error) {
	c := splitConfigJS{}

	c.defaultOutputPort = config["defaultOutputPort"].(string)
	rules := config["rules"].([]interface{})
	c.rules = make([]splitRuleJS, len(rules))

	for i := 0; i < len(rules); i++ {
		curRuleMap := rules[i].(map[string]interface{})
		newRule := splitRuleJS{}

		newRule.outputPort = curRuleMap["outputPort"].(string)
		newRule.jsCondition = curRuleMap["jsCondition"].(string)

		c.rules[i] = newRule
	}

	return c, nil
}

func (f *splitOnFieldJS) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
	defer out.Close()

	//Parse/read config options
	splitOpts, err := f.buildConfig(config)
	if err != nil {
		panic("Error parsing/building split config")
	}

	//Open the data stream
	reader := in.PortReader(dataflow.DEFAULT_INPUT_PORT_NAME)
	err = reader.Open()
	if err != nil {
		panic("Unable to read record from input")
	}

	//JS Template - We will wrap the condition express in a function that can be called more easily
	jsFuncTemplate, err := template.New("someName").Parse(`
		var {{.FuncName}} = function(data) {
			if ({{.Condition}}) {
				return true;
			} else {
				return false;
			}
		};
	`)

	vm := otto.New()

	//Parse all the JS rules
	for i := range splitOpts.rules {
		curRule := &(splitOpts.rules[i])

		var b bytes.Buffer
		templateVars := make(map[string]string)
		templateVars["Condition"] = curRule.jsCondition
		templateVars["FuncName"] = fmt.Sprintf("splitRuleFunc_%d", i)
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
		matchFound := false
		for i := range splitOpts.rules {
			curRule := splitOpts.rules[i]
			recData, _ := rec.GetAsRecord()
			value, err := vm.Call(curRule.jsFuncName, nil, recData)
			if err != nil {
				panic(err)
			}

			matches, err := value.ToBoolean()
			if err != nil {
				panic(err)
			}

			if matches {
				matchFound = true
				out.WriteRecord(curRule.outputPort, &recVal)
				break
			}
		}

		//No matches!
		if !matchFound {
			out.WriteRecord(splitOpts.defaultOutputPort, &recVal)
		}

		/* END - The actual work */
	}

	return nil
}
