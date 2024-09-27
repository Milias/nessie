/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.events.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.model.CommitMeta;

/**
 * Base interface for all events produced by Nessie.
 *
 * @see CommitEvent
 * @see MergeEvent
 * @see TransplantEvent
 * @see ReferenceCreatedEvent
 * @see ReferenceUpdatedEvent
 * @see ReferenceDeletedEvent
 * @see ContentStoredEvent
 * @see ContentRemovedEvent
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = CommitEvent.class, name = "COMMIT"),
  @JsonSubTypes.Type(value = MergeEvent.class, name = "MERGE"),
  @JsonSubTypes.Type(value = TransplantEvent.class, name = "TRANSPLANT"),
  @JsonSubTypes.Type(value = ReferenceCreatedEvent.class, name = "REFERENCE_CREATED"),
  @JsonSubTypes.Type(value = ReferenceUpdatedEvent.class, name = "REFERENCE_UPDATED"),
  @JsonSubTypes.Type(value = ReferenceDeletedEvent.class, name = "REFERENCE_DELETED"),
  @JsonSubTypes.Type(value = ContentStoredEvent.class, name = "CONTENT_STORED"),
  @JsonSubTypes.Type(value = ContentRemovedEvent.class, name = "CONTENT_REMOVED")
})
public interface Event {

  /** The type of the event. */
  EventType getType();

  /**
   * The id of the event. The UUID is generated by the event emitter. No assumptions should be made
   * about the version of the UUID.
   */
  UUID getId();

  /** The id of the event, as a string for convenience. */
  @Value.Lazy
  @JsonIgnore
  default String getIdAsText() {
    return getId().toString();
  }

  /**
   * The id of the repository. This is configured on a per-instance basis.
   *
   * <p>See configuration option: {@code nessie.version.store.persist.repository-id}.
   */
  String getRepositoryId();

  /**
   * The time the event was created. The time is generated by the event emitter and is based on its
   * internal wall clock time.
   */
  @JsonSerialize(using = CommitMeta.InstantSerializer.class)
  @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
  Instant getEventCreationTimestamp();

  /**
   * The user that initiated the event. The user is set by the event emitter and is based on the
   * authenticated user that performed the action that caused the event to be emitted.
   *
   * <p>If authentication is disabled, this will be empty.
   */
  Optional<String> getEventInitiator();

  /** A map of properties that can be used to add additional information to the event. */
  @JsonAnyGetter
  Map<String, Object> getProperties();
}
