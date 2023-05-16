/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination_async;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnStartFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.RecordSizeEstimator;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class AsyncStreamConsumer implements AirbyteMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncStreamConsumer.class);

  private static final String NON_STREAM_STATE_IDENTIFIER = "GLOBAL";
  private final Consumer<AirbyteMessage> outputRecordCollector;
  private final OnStartFunction onStart;
  private final OnCloseFunction2 onClose;
  private final ConfiguredAirbyteCatalog catalog;
  private final CheckedFunction<JsonNode, Boolean, Exception> isValidRecord;

  private final BufferManager bufferManager;
  private final BufferManagerEnqueue bufferManagerEnqueue;
  private final UploadWorkers uploadWorkers;
  private final Set<StreamDescriptor> streamNames;
  private final IgnoredRecordsTracker ignoredRecordsTracker;

  private boolean hasStarted;
  private boolean hasClosed;

  public AsyncStreamConsumer(final Consumer<AirbyteMessage> outputRecordCollector,
                             final OnStartFunction onStart,
                             final OnCloseFunction2 onClose,
                             final StreamDestinationFlusher flusher,
                             final ConfiguredAirbyteCatalog catalog,
                             final CheckedFunction<JsonNode, Boolean, Exception> isValidRecord,
                             final BufferManager bufferManager) {
    hasStarted = false;
    hasClosed = false;

    this.outputRecordCollector = outputRecordCollector;
    this.onStart = onStart;
    this.onClose = onClose;
    this.catalog = catalog;
    this.isValidRecord = isValidRecord;
    this.bufferManager = bufferManager;
    bufferManagerEnqueue = bufferManager.getBufferManagerEnqueue();
    streamNames = StreamDescriptorUtils.fromConfiguredCatalog(catalog);
    ignoredRecordsTracker = new IgnoredRecordsTracker();
    uploadWorkers = new UploadWorkers(bufferManager.getBufferManagerDequeue(), flusher);
  }

  @Override
  public void start() throws Exception {
    Preconditions.checkState(!hasStarted, "Consumer has already been started.");
    hasStarted = true;

    uploadWorkers.start();

    LOGGER.info("{} started.", AsyncStreamConsumer.class);
    onStart.call();
  }

  @Override
  public void accept(final AirbyteMessage message) throws Exception {
    Preconditions.checkState(hasStarted, "Cannot accept records until consumer has started");
    /*
     * intentionally putting extractStream outside the buffer manager so that if in the future we want
     * to try to use a threadpool to partial deserialize to get record type and stream name, we can do
     * it without touching buffer manager.
     */
    extractStream(message)
        .ifPresent(streamDescriptor -> bufferManagerEnqueue.addRecord(streamDescriptor, message));
  }

  @Override
  public void close() throws Exception {
    Preconditions.checkState(hasStarted, "Cannot close; has not started.");
    Preconditions.checkState(!hasClosed, "Has already closed.");
    hasClosed = true;

    // assume the closing upload workers will flush all accepted records.
    onClose.call();
    uploadWorkers.close();
    // bufferManager.close();
    ignoredRecordsTracker.report();
    LOGGER.info("{} closed.", AsyncStreamConsumer.class);
  }

  // todo (cgardens) - handle global state.
  /**
   * Extract the stream from the message, ff the message is a record or state. Otherwise, we don't
   * care.
   *
   * @param message message to extract stream from
   * @return stream descriptor if the message is a record or state, otherwise empty. In the case of
   *         global state messages the stream descriptor is hardcoded
   */
  private Optional<StreamDescriptor> extractStream(final AirbyteMessage message) {
    if (message.getType() == Type.RECORD) {
      final StreamDescriptor streamDescriptor = new StreamDescriptor()
          .withNamespace(message.getRecord().getNamespace())
          .withName(message.getRecord().getStream());

      validateRecord(message, streamDescriptor);

      return Optional.of(streamDescriptor);
    } else if (message.getType() == Type.STATE) {
      if (message.getState().getType() == AirbyteStateType.STREAM) {
        return Optional.of(message.getState().getStream().getStreamDescriptor());
      } else {
        return Optional.of(new StreamDescriptor().withNamespace(NON_STREAM_STATE_IDENTIFIER).withNamespace(NON_STREAM_STATE_IDENTIFIER));
      }
    } else {
      return Optional.empty();
    }
  }

  private void validateRecord(final AirbyteMessage message, final StreamDescriptor streamDescriptor) {
    // if stream is not part of list of streams to sync to then throw invalid stream exception
    if (!streamNames.contains(streamDescriptor)) {
      throwUnrecognizedStream(catalog, message);
    }

    trackerIsValidRecord(message, streamDescriptor);
  }

  private void trackerIsValidRecord(final AirbyteMessage message, final StreamDescriptor streamDescriptor) {
    // todo (cgardens) - is valid should also move inside the tracker, but don't want to blow up more
    // constructors right now.
    try {

      if (!isValidRecord.apply(message.getRecord().getData())) {
        ignoredRecordsTracker.addRecord(streamDescriptor, message);
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void throwUnrecognizedStream(final ConfiguredAirbyteCatalog catalog, final AirbyteMessage message) {
    throw new IllegalArgumentException(
        String.format("Message contained record from a stream that was not in the catalog. \ncatalog: %s , \nmessage: %s",
            Jsons.serialize(catalog), Jsons.serialize(message)));
  }

  public static class BufferManager implements AutoCloseable {

    Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> buffers;

    BufferManagerEnqueue bufferManagerEnqueue;
    BufferManagerDequeue bufferManagerDequeue;

    public BufferManager() {
      buffers = new HashMap<>();
      bufferManagerEnqueue = new BufferManagerEnqueue(buffers);
      bufferManagerDequeue = new BufferManagerDequeue(buffers);
    }

    public BufferManagerEnqueue getBufferManagerEnqueue() {
      return bufferManagerEnqueue;
    }

    public BufferManagerDequeue getBufferManagerDequeue() {
      return bufferManagerDequeue;
    }

    /**
     * Closing a queue will flush all items from it. For this reason, this method needs to be called
     * after {@link UploadWorkers#close()}. This allows the upload workers to make sure all items in the
     * queue has been flushed.
     */
    @Override
    public void close() throws Exception {
      buffers.forEach(((streamDescriptor, queue) -> queue.clear()));
      log.info("Buffers cleared..");
    }

  }

  static class BufferManagerEnqueue {

    private final RecordSizeEstimator recordSizeEstimator;
    private final Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> buffers;

    public BufferManagerEnqueue(final Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> buffers) {
      this.buffers = buffers;
      recordSizeEstimator = new RecordSizeEstimator();
    }

    public void addRecord(final StreamDescriptor streamDescriptor, final AirbyteMessage message) {
      // todo (cgardens) - share the total memory across multiple queues.
      final long availableMemory = (long) (Runtime.getRuntime().maxMemory() * 0.8);
      LOGGER.info("available memory: " + availableMemory);

      // todo (cgardens) - replace this with fancy logic to make sure we don't oom.
      if (!buffers.containsKey(streamDescriptor)) {
        buffers.put(streamDescriptor, new MemoryBoundedLinkedBlockingQueue<>(1024 * 1024 * 50)); // todo
      }

      // todo (cgardens) - handle estimating state message size.
      final long messageSize = message.getType() == Type.RECORD ? recordSizeEstimator.getEstimatedByteSize(message.getRecord()) : 1024;
      var queue = buffers.get(streamDescriptor);
      queue.offer(message, messageSize);
    }

  }

  // todo (cgardens) - make all the metadata methods more efficient.
  static class BufferManagerDequeue {

    Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> buffers;

    public BufferManagerDequeue(final Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> buffers) {
      this.buffers = buffers;
    }

    public Map<StreamDescriptor, MemoryBoundedLinkedBlockingQueue<AirbyteMessage>> getBuffers() {
      return new HashMap<>(buffers);
    }

    public MemoryBoundedLinkedBlockingQueue<AirbyteMessage> getBuffer(final StreamDescriptor streamDescriptor) {
      return buffers.get(streamDescriptor);
    }

    public long getTotalGlobalQueueSizeInMb() {
      return buffers.values().stream().map(MemoryBoundedLinkedBlockingQueue::getCurrentMemoryUsage).mapToLong(Long::longValue).sum();
    }

    public long getQueueSizeInMb(final StreamDescriptor streamDescriptor) {
      return getBuffer(streamDescriptor).getCurrentMemoryUsage();
    }

    public Optional<Instant> getTimeOfLastRecord(final StreamDescriptor streamDescriptor) {
      return getBuffer(streamDescriptor).getTimeOfLastMessage();
    }

  }

}
