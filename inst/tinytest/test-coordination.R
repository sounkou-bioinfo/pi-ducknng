open_store <- getFromNamespace("coordination_open", "piducknng")
close_store <- getFromNamespace("coordination_close", "piducknng")
dispatch <- getFromNamespace("coordination_dispatch", "piducknng")

clock <- 1000000
now <- function() clock
database <- tempfile(fileext = ".duckdb")
store <- open_store(database, now = now)

register <- function(agent, instance, operation = paste0("register-", instance)) {
  dispatch(store, "register", list(
    project_id = "project-one",
    agent_id = agent,
    instance_id = instance,
    operation_key = operation,
    adapter_kind = "test",
    delivery_capability = "durable_test",
    ttl_ms = 30000
  ))
}

alice <- register("alice", "alice-1")
replayed_alice <- register("alice", "alice-1")
expect_identical(replayed_alice$registration_id, alice$registration_id)
expect_true(replayed_alice$replayed)

sent <- dispatch(store, "send", list(
  registration_id = alice$registration_id,
  recipient_agent_id = "bob",
  idempotency_key = "message-1",
  content = "coordinate this"
))
replayed_send <- dispatch(store, "send", list(
  registration_id = alice$registration_id,
  recipient_agent_id = "bob",
  idempotency_key = "message-1",
  content = "coordinate this"
))
expect_identical(replayed_send$message_id, sent$message_id)
expect_true(replayed_send$replayed)
expect_error(dispatch(store, "send", list(
  registration_id = alice$registration_id,
  recipient_agent_id = "bob",
  idempotency_key = "message-1",
  content = "different content"
)), pattern = "idempotency_key")

bob <- register("bob", "bob-1")
first_delivery <- dispatch(store, "receive", list(
  registration_id = bob$registration_id,
  visibility_timeout_ms = 1000
))$messages[[1L]]
expect_identical(first_delivery$message_id, sent$message_id)

close_store(store)
clock <- clock + 1001
store <- open_store(database, now = now)
second_delivery <- dispatch(store, "receive", list(
  registration_id = bob$registration_id,
  visibility_timeout_ms = 1000
))$messages[[1L]]
expect_identical(second_delivery$message_id, sent$message_id)
expect_false(identical(second_delivery$receipt_token, first_delivery$receipt_token))

acknowledged <- dispatch(store, "ack", list(
  registration_id = bob$registration_id,
  receipt_token = second_delivery$receipt_token,
  delivery_ref = "test-entry"
))
replayed_ack <- dispatch(store, "ack", list(
  registration_id = bob$registration_id,
  receipt_token = second_delivery$receipt_token,
  delivery_ref = "test-entry"
))
expect_true(acknowledged$acknowledged)
expect_identical(acknowledged$delivery_capability, "durable_test")
expect_identical(acknowledged$receiver_instance_id, "bob-1")
expect_true(replayed_ack$replayed)
expect_length(dispatch(store, "receive", list(
  registration_id = bob$registration_id
))$messages, 0L)

alice_lease <- dispatch(store, "reserve", list(
  registration_id = alice$registration_id,
  resource = "file:///tmp/project/data",
  operation_key = "reserve-data",
  ttl_ms = 5000
))
replayed_lease <- dispatch(store, "reserve", list(
  registration_id = alice$registration_id,
  resource = "file:///tmp/project/data",
  operation_key = "reserve-data",
  ttl_ms = 5000
))
expect_identical(replayed_lease$lease_id, alice_lease$lease_id)
expect_true(replayed_lease$replayed)
expect_error(dispatch(store, "reserve", list(
  registration_id = bob$registration_id,
  resource = "file:///tmp/project",
  operation_key = "reserve-parent",
  ttl_ms = 5000
)), pattern = "conflicts")

clock <- clock + 5001
expired_replay <- dispatch(store, "reserve", list(
  registration_id = alice$registration_id,
  resource = "file:///tmp/project/data",
  operation_key = "reserve-data",
  ttl_ms = 5000
))
expect_false(expired_replay$active)
bob_lease <- dispatch(store, "reserve", list(
  registration_id = bob$registration_id,
  resource = "file:///tmp/project",
  operation_key = "reserve-parent-after-expiry",
  ttl_ms = 5000
))
expect_true(bob_lease$fencing_value > alice_lease$fencing_value)
stale_release <- dispatch(store, "release", list(
  registration_id = alice$registration_id,
  lease_id = alice_lease$lease_id
))
expect_true(stale_release$released)
renewed <- dispatch(store, "reserve", list(
  registration_id = bob$registration_id,
  resource = "file:///tmp/project",
  lease_id = bob_lease$lease_id,
  ttl_ms = 10000
))
expect_true(renewed$renewed)

agents <- dispatch(store, "list_agents", list(
  registration_id = alice$registration_id
))$agents
expect_equal(vapply(agents, `[[`, "", "agent_id"), c("alice", "bob"))

unregistered <- dispatch(store, "unregister", list(
  registration_id = bob$registration_id
))
replayed_unregister <- dispatch(store, "unregister", list(
  registration_id = bob$registration_id
))
expect_true(unregistered$unregistered)
expect_true(replayed_unregister$replayed)
resumed_bob <- register("bob", "bob-1")
expect_false(identical(resumed_bob$registration_id, bob$registration_id))
dispatch(store, "unregister", list(registration_id = resumed_bob$registration_id))
expect_equal(
  vapply(dispatch(store, "list_agents", list(
    registration_id = alice$registration_id
  ))$agents, `[[`, "", "agent_id"),
  "alice"
)

close_store(store)
unlink(c(database, paste0(database, ".wal")))

clock <- 2000000
limited_database <- tempfile(fileext = ".duckdb")
store <- open_store(
  limited_database,
  now = now,
  acknowledged_retention_ms = 86400000,
  max_pending_per_mailbox = 1
)
sender <- register("sender", "sender-1")
first <- dispatch(store, "send", list(
  registration_id = sender$registration_id,
  recipient_agent_id = "recipient",
  idempotency_key = "retained-key",
  content = "first"
))
expect_error(dispatch(store, "send", list(
  registration_id = sender$registration_id,
  recipient_agent_id = "recipient",
  idempotency_key = "blocked-key",
  content = "blocked"
)), pattern = "pending message limit")
recipient <- register("recipient", "recipient-1")
delivery <- dispatch(store, "receive", list(
  registration_id = recipient$registration_id
))$messages[[1L]]
dispatch(store, "ack", list(
  registration_id = recipient$registration_id,
  receipt_token = delivery$receipt_token
))
clock <- clock + 86400001
sender <- register("sender", "sender-1", "register-sender-after-retention")
after_retention <- dispatch(store, "send", list(
  registration_id = sender$registration_id,
  recipient_agent_id = "recipient",
  idempotency_key = "retained-key",
  content = "first"
))
expect_false(identical(after_retention$message_id, first$message_id))
close_store(store)
unlink(c(limited_database, paste0(limited_database, ".wal")))
