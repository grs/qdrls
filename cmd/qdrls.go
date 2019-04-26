package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
        "text/tabwriter"

	"pack.ag/amqp"
)

func alias(attribute string) string {
	aliases := map[string]string {
		"linkType":"type",
		"linkDir":"dir",
		"connectionId":"conn",
		"identity":"id",
		"owningAddr":"addr",
		"capacity":"cpcty",
		"linkName": "name",
		"undeliveredCount":"undel",
		"unsettledCount":"unsett",
		"deliveryCount":"del",
		"acceptedCount":"acc",
		"releasedCount":"rel",
		"modifiedCount":"mod",
		"rejectedCount":"rej",
		"presettledCount":"presett",
		"droppedPresettledCount":"psdrop"}
	alt := aliases[attribute]
	if len(alt) > 0 {
		return alt
	} else {
		return attribute
	}
}

func defaultAttributes(typename string) []interface{} {
	linkAttributes := [...]interface{} {"linkType", "linkDir", "connectionId", "identity", "peer", "owningAddr", "capacity", "undeliveredCount", "unsettledCount", "deliveryCount", "acceptedCount", "releasedCount", "modifiedCount", "rejectedCount", "presettledCount", "droppedPresettledCount"}
	return linkAttributes[:]
}

func attributeNames(specified string, typename string) []interface{} {
	if len(specified) > 0 {
		list := strings.Split(specified, ",")
		copy := make([]interface{}, len(list))
		for i, v := range list {
			copy[i] = v
		}
		return copy[:]
	} else {
		return defaultAttributes(typename)
	}
}

func qualifiedType(typename string) string {
	if typename == "link" || typename == "address" {
		return fmt.Sprintf("org.apache.qpid.dispatch.router.%s",  typename)
	} else {
		return fmt.Sprintf("org.apache.qpid.dispatch.%s",  typename)
	}
}

func divider(items []interface{}) string {
	s := make([]string, len(items))
	for i := range items {
		o := alias(fmt.Sprintf("%s", items[i]))
		s[i] = strings.Repeat("=", len(o))
	}
	return strings.Join(s, "\t")
}

func stringify(items []interface{}) []string {
	s := make([]string, len(items))
	for i := range items {
		s[i] = fmt.Sprintf("%v", items[i])
	}
	return s
}

func tabbed(items []interface{}) string {
	s := stringify(items)
	return strings.Join(s, "\t")
}

func header(items []interface{}) string {
	s := stringify(items)
	for i, v := range s {
		s[i] = strings.ToUpper(alias(v))
	}
	return strings.Join(s, "\t")
}

func authOption(username string, password string) amqp.ConnOption {
	if (len(password) > 0 && len(username) > 0) {
		return amqp.ConnSASLPlain(username, password)
	} else {
		return amqp.ConnSASLAnonymous()
	}
}

func main() {
	url := flag.String("url", "amqp://localhost:5672", "URL to connect to")
	typename := flag.String("type", "link", "Type of the entities to list")
	attributes := flag.String("attributes", "", "Comma separated list of attributes to display")
	username := flag.String("username", "", "User to connect as")
	password := flag.String("password", "", "Password to connect with")
	flag.Parse()

	client, err := amqp.Dial(*url, authOption(*username, *password), amqp.ConnMaxFrameSize(4294967295))
	if err != nil {
		log.Fatal("Failed to connect to ", *url, ": ", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Failed to create session:", err)
	}

	ctx := context.Background()

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(""),
		amqp.LinkAddressDynamic(),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Failed to create receiver:", err)
	}
	sender, err := session.NewSender(
		amqp.LinkTargetAddress("$management"),
	)
	if err != nil {
		log.Fatal("Failed to create sender:", err)
	}

	var request amqp.Message
	var properties amqp.MessageProperties
	properties.ReplyTo = receiver.Address()
	properties.CorrelationID = 1
	request.Properties = &properties
	request.ApplicationProperties = make(map[string]interface{})
	request.ApplicationProperties["operation"] = "QUERY"
	request.ApplicationProperties["entityType"] = qualifiedType(*typename)
	var body = make(map[string]interface{})
	body["attributeNames"] = attributeNames(*attributes, *typename)
	request.Value = body

	err = sender.Send(ctx, &request)
	if err != nil {
		log.Fatal("Could not send request:", err)
	}

	sender.Close(ctx)

	response, err := receiver.Receive(ctx)
	if err != nil {
		log.Fatal("Failed to receive reponse:", err)
	}
	response.Accept()

	if status, ok := response.ApplicationProperties["statusCode"].(int32); ok && status == 200 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if top, ok := response.Value.(map[string]interface{}); ok {
			fields := top["attributeNames"].([]interface{})
			fmt.Fprintln(w, header(fields))
			results := top["results"].([]interface{})
			for _, r := range results {
				o := r.([]interface{})
				fmt.Fprintln(w, tabbed(o))
			}
			w.Flush()
		} else {
			fmt.Printf("Bad response: %s\n", response.Value)
		}
	} else {
		fmt.Printf("ERROR: %s\n", response.ApplicationProperties["statusDescription"])
	}
	receiver.Close(ctx)
}
