package nmos

// NMOS standard URN constants for IS-04 v1.3, IS-05 v1.1, and IS-07 v1.0.
const (
	FormatData         = "urn:x-nmos:format:data"
	DeviceTypeGeneric  = "urn:x-nmos:device:generic"
	TransportWebsocket = "urn:x-nmos:transport:websocket"

	ControlSRv11    = "urn:x-nmos:control:sr-ctrl/v1.1"
	ControlSRv10    = "urn:x-nmos:control:sr-ctrl/v1.0"
	ControlEvents10 = "urn:x-nmos:control:events/v1.0"

	EventTypeBoolean = "boolean"
	MediaTypeJSON    = "application/json"
)

// --- IS-04 v1.3 Resource Models ---

type APIEndpoint struct {
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Protocol      string `json:"protocol"`
	Authorization bool   `json:"authorization"`
}

type NodeAPIInfo struct {
	Versions  []string      `json:"versions"`
	Endpoints []APIEndpoint `json:"endpoints"`
}

type NodeClock struct {
	Name    string `json:"name"`
	RefType string `json:"ref_type"`
}

type NodeInterface struct {
	Name      string  `json:"name"`
	ChassisID *string `json:"chassis_id"`
	PortID    string  `json:"port_id"`
}

type NodeResource struct {
	ID          string              `json:"id"`
	Version     string              `json:"version"`
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
	Href        string              `json:"href"`
	Hostname    string              `json:"hostname"`
	API         NodeAPIInfo         `json:"api"`
	Caps        map[string]any      `json:"caps"`
	Services    []any               `json:"services"`
	Clocks      []NodeClock         `json:"clocks"`
	Interfaces  []NodeInterface     `json:"interfaces"`
}

type DeviceControl struct {
	Type          string `json:"type"`
	Href          string `json:"href"`
	Authorization bool   `json:"authorization"`
}

type DeviceResource struct {
	ID          string              `json:"id"`
	Version     string              `json:"version"`
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
	Type        string              `json:"type"`
	NodeID      string              `json:"node_id"`
	Senders     []string            `json:"senders"`
	Receivers   []string            `json:"receivers"`
	Controls    []DeviceControl     `json:"controls"`
}

type SourceResource struct {
	ID          string              `json:"id"`
	Version     string              `json:"version"`
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
	Format      string              `json:"format"`
	Caps        map[string]any      `json:"caps"`
	DeviceID    string              `json:"device_id"`
	Parents     []string            `json:"parents"`
	ClockName   *string             `json:"clock_name"`
	EventType   string              `json:"event_type"`
}

type FlowResource struct {
	ID          string              `json:"id"`
	Version     string              `json:"version"`
	Label       string              `json:"label"`
	Description string              `json:"description"`
	Tags        map[string][]string `json:"tags"`
	Format      string              `json:"format"`
	MediaType   string              `json:"media_type"`
	EventType   string              `json:"event_type"`
	SourceID    string              `json:"source_id"`
	DeviceID    string              `json:"device_id"`
	Parents     []string            `json:"parents"`
	Caps        map[string]any      `json:"caps"`
}

type SenderSubscription struct {
	ReceiverID *string `json:"receiver_id"`
	Active     bool    `json:"active"`
}

type SenderResource struct {
	ID                string              `json:"id"`
	Version           string              `json:"version"`
	Label             string              `json:"label"`
	Description       string              `json:"description"`
	Tags              map[string][]string `json:"tags"`
	FlowID            string              `json:"flow_id"`
	Transport         string              `json:"transport"`
	DeviceID          string              `json:"device_id"`
	ManifestHref      *string             `json:"manifest_href"`
	InterfaceBindings []string            `json:"interface_bindings"`
	Subscription      SenderSubscription  `json:"subscription"`
}

type ReceiverCaps struct {
	MediaTypes []string `json:"media_types"`
	EventTypes []string `json:"event_types,omitempty"`
}

type ReceiverSubscription struct {
	SenderID *string `json:"sender_id"`
	Active   bool    `json:"active"`
}

type ReceiverResource struct {
	ID                string               `json:"id"`
	Version           string               `json:"version"`
	Label             string               `json:"label"`
	Description       string               `json:"description"`
	Tags              map[string][]string  `json:"tags"`
	DeviceID          string               `json:"device_id"`
	Format            string               `json:"format"`
	Transport         string               `json:"transport"`
	InterfaceBindings []string             `json:"interface_bindings"`
	Caps              ReceiverCaps         `json:"caps"`
	Subscription      ReceiverSubscription `json:"subscription"`
}

// --- IS-05 v1.1 Connection Management Models ---

type ActivationParams struct {
	Mode          *string `json:"mode"`
	RequestedTime *string `json:"requested_time"`
	ActivationTime *string `json:"activation_time,omitempty"`
}

// WebSocketTransportParams models IS-05 + IS-07 WebSocket transport parameters.
// Fields can be concrete strings/booleans or "auto" in staged requests.
type WebSocketTransportParams struct {
	ConnectionURI           any    `json:"connection_uri,omitempty"`
	ConnectionAuthorization any    `json:"connection_authorization,omitempty"`
	ExtIS07SourceID         string `json:"ext_is_07_source_id,omitempty"`
	ExtIS07RestAPIURL       string `json:"ext_is_07_rest_api_url,omitempty"`
}

type SenderConnectionState struct {
	ReceiverID      *string                    `json:"receiver_id"`
	MasterEnable    bool                       `json:"master_enable"`
	Activation      ActivationParams           `json:"activation"`
	TransportParams []WebSocketTransportParams `json:"transport_params"`
}

type TransportFileState struct {
	Data *string `json:"data"`
	Type *string `json:"type"`
}

type ReceiverWebSocketParams struct {
	ConnectionAuthorization any     `json:"connection_authorization"`
	ConnectionURI           *string `json:"connection_uri"`
	ExtIS07RestAPIURL       *string `json:"ext_is_07_rest_api_url"`
	ExtIS07SourceID         *string `json:"ext_is_07_source_id"`
}

type ReceiverConnectionState struct {
	SenderID        *string                   `json:"sender_id"`
	MasterEnable    bool                      `json:"master_enable"`
	Activation      ActivationParams          `json:"activation"`
	TransportFile   TransportFileState        `json:"transport_file"`
	TransportParams []ReceiverWebSocketParams `json:"transport_params"`
}

type SenderPatchRequest struct {
	ReceiverID      *string                    `json:"receiver_id,omitempty"`
	HasReceiverID   bool                       `json:"-"`
	MasterEnable    *bool                      `json:"master_enable,omitempty"`
	Activation      *ActivationParams          `json:"activation,omitempty"`
	TransportParams []WebSocketTransportParams `json:"transport_params,omitempty"`
}

type ConstraintRule struct {
	Enum []any `json:"enum,omitempty"`
}

type BulkSenderItem struct {
	ID     string         `json:"id"`
	Params map[string]any `json:"params"`
}

type BulkResponseItem struct {
	ID    string `json:"id"`
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`
}

// --- IS-07 v1.0 Event & Tally Models ---

type EventTypeBooleanSchema struct {
	Type string `json:"type"` // Always "boolean"
}

type EventIdentity struct {
	SourceID string `json:"source_id"`
	FlowID   string `json:"flow_id,omitempty"`
}

type EventTiming struct {
	CreationTimestamp string `json:"creation_timestamp"`
	OriginTimestamp   string `json:"origin_timestamp,omitempty"`
	ActionTimestamp   string `json:"action_timestamp,omitempty"`
}

type BooleanPayload struct {
	Value bool `json:"value"`
}

type IS07StateMessage struct {
	Identity    EventIdentity  `json:"identity"`
	EventType   string         `json:"event_type"`
	Timing      EventTiming    `json:"timing"`
	Payload     BooleanPayload `json:"payload"`
	MessageType string         `json:"message_type"` // "state"
}

type IS07LifecycleMessage struct {
	Identity    EventIdentity `json:"identity"`
	Timing      EventTiming   `json:"timing"`
	MessageType string        `json:"message_type"` // "reboot" or "shutdown"
}

type IS07HealthResponse struct {
	Timing      EventTiming `json:"timing"`
	MessageType string      `json:"message_type"` // "health"
}

type IS07WSClientCommand struct {
	Command   string   `json:"command"`             // "subscription" or "health"
	Sources   []string `json:"sources,omitempty"`   // For "subscription"
	Timestamp string   `json:"timestamp,omitempty"` // For "health"
}
