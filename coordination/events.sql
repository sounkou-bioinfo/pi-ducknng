-- Handler for the /events route: one mailbox's wake-up hints as Server-Sent
-- Events. The topic register returned is the capability; a topic that names
-- no registered mailbox gets no subscription, which answers 404. Each hint is
-- an event named "mail" whose data is the topic, and it carries no mail.
SELECT m.event_relay_url AS url, q.topic AS topic, 'mail' AS event
FROM coordination_meta m, (SELECT ducknng_http_query_param('topic') AS topic) q
WHERE m.event_relay_url IS NOT NULL
  AND regexp_full_match(coalesce(q.topic, ''), '[0-9a-f]{32}')
  AND EXISTS (
    SELECT 1 FROM coordination_agents a
    WHERE coord_topic(a.project_id, a.agent_id) = q.topic
  )
