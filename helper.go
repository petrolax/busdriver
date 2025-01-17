package busdriver

import "fmt"

func MakeTopic(serviceName, topic string) string {
	return fmt.Sprintf("%s:%s", serviceName, topic)
}
