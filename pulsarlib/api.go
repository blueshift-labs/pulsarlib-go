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
		return fmt.Errorf("InitMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in creating tenant, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func DeleteTenant(tenantID string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in deleting tenant, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func CreateNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in creating namespace, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func DeleteNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in deleting namespace, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func UnloadNamespace(tenantID string, namespace string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	unloadUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in unloading namespace, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func ListTenants() ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
		Path:   fmt.Sprintf("admin/v2/tenants"),
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
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("InitMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in creating topic, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func DeleteTopic(tenantID string, namespace string, topic string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in deleting topic, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func ListTopics(tenantID string, namespace string) ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("InitMessaging not called yet")
	}

	createUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in creating topic, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func DeletePartionedTopic(tenantID string, namespace string, topic string) error {
	//Check if InitMessaging was done prior to this call
	if msging == nil {
		return fmt.Errorf("InitMessaging not called yet")
	}

	deleteUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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
		return fmt.Errorf("Error in deleting topic, Http Response [%v]", resp.StatusCode)
	}
	return nil
}

func ListPartionedTopics(tenantID string, namespace string) ([]string, error) {
	result := make([]string, 0)

	getUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", msging.pulsarHost, "8080"),
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

func CreateSubscriptionOnTopic(accountUUID, clusterName, indexName, position string) error {
	requestBody := make(map[string]interface{})
	if strings.EqualFold(position, "earliest") {
		requestBody = messageIDEarliest
	} else if strings.EqualFold(position, "latest") {
		requestBody = messageIDLatest
	} else {
		return errors.New("invalid position - must be either 'earliest' or 'latest'")
	}

	putUrl := (&url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", "localhost", "8080"),
		Path:   fmt.Sprintf("admin/v2/persistent/indexing/%s/events.json/subscription/elasticsearch.%s.%s", accountUUID, clusterName, indexName),
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

	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Error in creating subscription, Http Response [%v]", resp.StatusCode)
	}
	return nil
}