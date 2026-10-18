// Package integration contains opt-in end-to-end tests that require real
// infrastructure (a Kafka broker and a Redis server). They are excluded from
// the default build and only compiled when the `integration` build tag is set:
//
//	go test -tags integration ./integration/... -run TestKafkaToBlockingRoundTrip -v
//
// See the "Integration test" section of the README for prerequisites.
package integration
