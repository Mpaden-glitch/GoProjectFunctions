package builtin

import (
	"bitbucket.org/primelogic_io/bitlantern/service/dataflow"
	"fmt"
	"regexp"
	"strconv"
)

func init() {
	//Register function with default builtin.FunctionProvider
	DefaultInstance().RegisterFunction(
		Function{
			FunctionSpec: dataflow.FunctionSpec{
				Key:           "splitOnField",
				Name:          "Split Data by Field",
				Description:   "Splits the input data",
				Category:      "Data",
				ExecutionMode: "sync",
				InputPorts:    nil,
				OutputPorts:   nil,
			},
			NewFunction: func() dataflow.Function {
				return &(splitOnField{})
			},
		})
}

type splitOnField struct {
}

type splitConfig struct {
	// splits contains the rules for data splitting. The first rule that matches the data will be used.
	rules []splitRule

	// defaultOutputPort specifies the output port into which to send data that doesn't match any other split rule
	defaultOutputPort string
}

type splitRule struct {
	field      string
	op         string
	value      interface{}
	valueType  string
	outputPort string
}

// buildConfig builds a splitConfig from the passed in map. The map must be in the form:
// {
//		"defaultOutputPort": "not poor",
//		"rules": [
//			{
//				"field":"balance".
//				"op": "<=",
//				"value": 50.00,
//				"valueType": "literal"
//				"outputPort": "poor"
//			}
//		]
// }
//
// rules.value can either be a value literal (ex: 100.00, "M", etc.) or a reference to another field in the record.
// rules.valueType accepts the values: "literal", "field"
func (f *splitOnField) buildConfig(config map[string]interface{}) (splitConfig, error) {
	c := splitConfig{}

	c.defaultOutputPort = config["defaultOutputPort"].(string)
	rules := config["rules"].([]interface{})
	c.rules = make([]splitRule, len(rules))

	for i := 0; i < len(rules); i++ {
		curRuleMap := rules[i].(map[string]interface{})
		newRule := splitRule{}

		newRule.outputPort = curRuleMap["outputPort"].(string)
		newRule.field = curRuleMap["field"].(string)
		newRule.op = curRuleMap["op"].(string)
		newRule.value = curRuleMap["value"]
		newRule.valueType = curRuleMap["valueType"].(string)

		c.rules[i] = newRule
	}

	return c, nil
}

func (f *splitOnField) matches(record *dataflow.Record, rule *splitRule) bool {
	leftValue, _ := record.Get(rule.field)
	var rightValue interface{}
	if rule.valueType == "literal" {
		rightValue = rule.value
	} else {
		//If not a literal, then the "value" is pointing to a field. Let's get the value from that field for this record
		rightValue, _ = record.Get(rule.value.(string))
	}

	switch leftValue.(type) {
	case string:
		return compareAsStrings(leftValue.(string), rule.op, rightValue)
	case int:
		return compareAsInts(leftValue.(int), rule.op, rightValue)
	case int8:
		return compareAsInts(int(leftValue.(int8)), rule.op, rightValue)
	case int16:
		return compareAsInts(int(leftValue.(int16)), rule.op, rightValue)
	case int32:
		return compareAsInts(int(leftValue.(int32)), rule.op, rightValue)
	case int64:
		return compareAsInts(int(leftValue.(int64)), rule.op, rightValue)
	case float32:
		return compareAsFloats(float64(leftValue.(float32)), rule.op, rightValue)
	case float64:
		return compareAsFloats(leftValue.(float64), rule.op, rightValue)
	default:
		panic("Unsupported type in split rule config")
	}

	return false
}

func compareAsStrings(left string, op string, right interface{}) bool {
	//Convert the second value (if not a string)
	var rightStr string
	switch right.(type) {
	case string:
		rightStr = right.(string)
	case int, int8, int16, int32, int64:
		rightStr = fmt.Sprintf("%d", right)
	case float32, float64:
		rightStr = fmt.Sprintf("%f", right)
	default:
		panic("Unsupported type in split rule config")
	}

	switch op {
	case "<":
		return left < rightStr
	case "<=":
		return left <= rightStr
	case "==":
		return left == rightStr
	case "!=":
		return left != rightStr
	case ">":
		return left > rightStr
	case ">=":
		return left >= rightStr
	case "regex":
		matched, _ := regexp.Match(rightStr, []byte(left))
		return matched
	default:
		panic("Unsupported split op")
	}
}

func compareAsInts(left int, op string, right interface{}) bool {
	//Convert the second value (if not a int)
	var rightInt int
	switch right.(type) {
	case string:
		rightInt, _ = strconv.Atoi(right.(string))
	case int:
		rightInt = right.(int)
	case int8, int16, int32, int64:
		rightInt, _ = strconv.Atoi(fmt.Sprintf("%d", right))
	case float32, float64:
		rightInt, _ = strconv.Atoi(fmt.Sprintf("%.0f", right))
	default:
		panic("Unsupported type in split rule config")
	}

	switch op {
	case "<":
		return left < rightInt
	case "<=":
		return left <= rightInt
	case "==":
		return left == rightInt
	case "!=":
		return left != rightInt
	case ">":
		return left > rightInt
	case ">=":
		return left >= rightInt
	default:
		panic("Unsupported split op")
	}
}

func compareAsFloats(left float64, op string, right interface{}) bool {
	//Convert the second value (if not a float)
	var rightFloat float64
	switch right.(type) {
	case string:
		rightFloat, _ = strconv.ParseFloat(right.(string), 64)
	case int, int8, int16, int32, int64:
		rightFloat, _ = strconv.ParseFloat(fmt.Sprintf("%d", right), 64)
	case float32:
		rightFloat = float64(right.(float32))
	case float64:
		rightFloat = right.(float64)
	default:
		panic("Unsupported type in split rule config")
	}

	switch op {
	case "<":
		return left < rightFloat
	case "<=":
		return left <= rightFloat
	case "==":
		return left == rightFloat
	case "!=":
		return left != rightFloat
	case ">":
		return left > rightFloat
	case ">=":
		return left >= rightFloat
	default:
		panic("Unsupported split op")
	}
}

func (f *splitOnField) Execute(in dataflow.InputReader, out dataflow.OutputWriter, config map[string]interface{}) error {
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
		destPort := splitOpts.defaultOutputPort
		for i := 0; i < len(splitOpts.rules); i++ {
			curRule := splitOpts.rules[i]
			if f.matches(&recVal, &curRule) {
				destPort = curRule.outputPort
			}
		}

		//Output
		out.WriteRecord(destPort, &recVal)

		/* END - The actual work */
	}

	return nil
}
