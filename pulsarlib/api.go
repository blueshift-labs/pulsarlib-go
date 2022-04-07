package pulsarlib

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var messageIDLatest = map[string]interface{}{
	"ledgerId":       9223372036854775807,
	"entryId":        9223372036854775807,
	"partitionIndex": -1,
}
var messageIDEarliest = map[string]interface{}{
	"ledgerId":       -1,
	"entryId":        -1,
	"partitionIndex": -1,
}

func CreateTenant(tenantID string, adminRoles []string, allowedClusters []string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/tenants/%s", tenantID),
	}).String()

	body := struct {
		AdminRoles      []string `json:"adminRoles,omitempty"`
		AllowedClusters []string `json:"allowedClusters,omitempty"`
	}{
		AdminRoles:      adminRoles,
		AllowedClusters: allowedClusters,
	}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, createUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in creating tenant, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in creating tenant, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func DeleteTenant(tenantID string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/tenants/%s", tenantID),
	}).String()

	req, err := http.NewRequest(http.MethodDelete, deleteUrl, bytes.NewBuffer(make([]byte, 0)))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in deleting tenant, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in deleting tenant, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}

	return nil
}

func CreateNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/namespaces/%s/%s", tenantID, namespace),
	}).String()

	body := make([]byte, 0)
	req, err := http.NewRequest(http.MethodPut, createUrl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in creating namespace, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in creating namespace, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func SetNamespaceRetention(tenantID string, namespace string, retentionInMB int64, retentionInMinutes int64) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	updateURL := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/namespaces/%s/%s/retention", tenantID, namespace),
	}).String()

	r := map[string]interface{}{
		"retentionTimeInMinutes": retentionInMinutes,
		"retentionSizeInMB":      retentionInMB,
	}
	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("Error creating request body for updating retetion")
	}
	req, err := http.NewRequest(http.MethodPost, updateURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in updating namespace retention, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in updating namespace retention, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func DeleteNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/namespaces/%s/%s", tenantID, namespace),
	}).String()

	body := make([]byte, 0)
	req, err := http.NewRequest(http.MethodDelete, deleteUrl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in deleting namespace, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in deleting namespace, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func UnloadNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	unloadUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/namespaces/%s/%s/unload", tenantID, namespace),
	}).String()

	body := make([]byte, 0)
	req, err := http.NewRequest(http.MethodPut, unloadUrl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in unloading namespace, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in unloading namespace, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func ListTenants() ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   "admin/v2/tenants",
	}).String()

	resp, err := http.Get(getUrl)
	if err != nil {
		return result, err
	}

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(respBuf, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func ListNamespaces(tenantID string) ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/namespaces/%s", tenantID),
	}).String()

	resp, err := http.Get(getUrl)
	if err != nil {
		return result, err
	}

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(respBuf, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func CreateTopic(tenantID string, namespace string, topic string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s", tenantID, namespace, topic),
	}).String()

	req, err := http.NewRequest(http.MethodPut, createUrl, nil)
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in creating topic, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in creating topic, Http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func DeleteTopic(tenantID string, namespace string, topic string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s", tenantID, namespace, topic),
	}).String()

	req, err := http.NewRequest(http.MethodDelete, deleteUrl, bytes.NewBuffer(make([]byte, 0)))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in deleting topic, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in deleting topic, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func ListTopics(tenantID string, namespace string) ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s", tenantID, namespace),
	}).String()

	resp, err := http.Get(getUrl)
	if err != nil {
		return result, err
	}

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(respBuf, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func CreatePartitionedTopic(tenantID string, namespace string, topic string, partitions int) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/partitions", tenantID, namespace, topic),
	}).String()

	jsonBody, err := json.Marshal(partitions)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, createUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in creating topic, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in creating topic, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func DeletePartionedTopic(tenantID string, namespace string, topic string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("initMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/partitions", tenantID, namespace, topic),
	}).String()

	req, err := http.NewRequest(http.MethodDelete, deleteUrl, bytes.NewBuffer(make([]byte, 0)))
	if err != nil {
		return err
	}

	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		//Making the API idempotent
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in deleting topic, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in deleting topic, http Response [%v]; and error [%s]", resp.StatusCode, string(respBody))
	}
	return nil
}

func ListPartionedTopics(tenantID string, namespace string) ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/partitioned", tenantID, namespace),
	}).String()

	resp, err := http.Get(getUrl)
	if err != nil {
		return result, err
	}

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(respBuf, &result)
	if err != nil {
		return result, err
	}
	return result, nil
}

func CreateSubscriptionOnTopic(tenantID, namespace, topic, subscription, position string) error {
	var requestBody map[string]interface{}
	if strings.EqualFold(position, "earliest") {
		requestBody = messageIDEarliest
	} else if strings.EqualFold(position, "latest") {
		requestBody = messageIDLatest
	} else {
		return errors.New("invalid position - must be either 'earliest' or 'latest'")
	}

	putUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/subscription/%s", tenantID, namespace, topic, subscription),
	}).String()

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return errors.New("failed to marshal request body while creating subscription on topic - " + err.Error())
	}
	req, err := http.NewRequest(http.MethodPut, putUrl, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in creating subscription, http Response [%v]; error while reading response body - [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in creating subscription, Http Response [%v]; and response - [%s]", resp.StatusCode, string(respBody))

	}
	return nil
}

func DeleteSubscriptionOnTopic(tenantID, namespace, topic, subscription string) error {
	putUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/subscription/%s", tenantID, namespace, topic, subscription),
	}).String()

	req, err := http.NewRequest(http.MethodDelete, putUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error in deleting subscription, http Response [%v]; error while reading response body [%s]", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("error in deleting subscription, http Response [%v]; response - [%s]", resp.StatusCode, string(respBody))
	}

	return nil
}

func GetSubscriptionsOnTopic(tenantID, namespace, topic string) (subscriptionsOnTopic []string, err error) {
	url := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/subscriptions", tenantID, namespace, topic),
	}).String()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, fmt.Errorf("error while reading pulsar response of subscriptions on topic - %s", err.Error())
	}

	if resp.StatusCode >= 400 {
		if err != nil {
			return nil, fmt.Errorf("status code %d Error while fetching subscriptions on topic - %s", resp.StatusCode, err.Error())
		}
		return nil, fmt.Errorf("status code %d Error while fetching subscriptions on topic - %s", resp.StatusCode, string(body))
	}

	err = json.Unmarshal(body, &subscriptionsOnTopic)
	if err != nil {
		return nil, fmt.Errorf("error while unmarshaling pulsar response of subscriptions on topic - %s", err.Error())
	}
	return

}

func ResetSubscriptionOnTopic(tenantID string, namespace string, topic string, subscription string, timestampMilliseconds int64) (err error) {
	url := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, msging.pulsarHttpPort),
		Path:   fmt.Sprintf("admin/v2/persistent/%s/%s/%s/subscription/%s/resetcursor/%d", tenantID, namespace, topic, subscription, timestampMilliseconds),
	}).String()

	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error while resetting subscription on topic - %s", err.Error())
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("status code %d Error while resetting subscriptions on topic - %s", resp.StatusCode, string(body))
	}
	return nil
}
