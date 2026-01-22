# DoSQL Integration Guide

This guide explains how to integrate DoSQL with DoLake for CDC (Change Data Capture) streaming to a lakehouse.

## Overview

DoSQL instances capture changes to their data and stream them to DoLake via WebSocket connections. DoLake then batches these changes, converts them to Parquet format, and writes them to R2 with Iceberg metadata.

```
+----------+     WebSocket      +----------+     Parquet     +----+
|  DoSQL   | -----------------> |  DoLake  | --------------> | R2 |
| (Source) |     CDC Events     | (Sink)   |    Iceberg      |    |
+----------+                    +----------+                 +----+
```

## Prerequisites

Before integrating, ensure you have:

1. A DoSQL Durable Object with CDC enabled
2. A DoLake Durable Object configured
3. R2 bucket for lakehouse storage
4. Both DOs in the same worker or accessible via service bindings

## Wrangler Configuration

Configure both DOs in your `wrangler.jsonc`:

```jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL", "class_name": "DoSQL" },
      { "name": "DOLAKE", "class_name": "DoLake" }
    ]
  },
  "r2_buckets": [
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "my-lakehouse" }
  ],
  "migrations": [
    { "tag": "v1", "new_classes": ["DoSQL", "DoLake"] }
  ]
}
```

## Connection Setup

### Step 1: Establish WebSocket Connection

From your DoSQL instance, connect to DoLake:

```typescript
import { type CDCEvent, type CDCBatchMessage, type AckMessage } from '@dotdo/dolake';

class DoSQL implements DurableObject {
  private dolakeWs: WebSocket | null = null;
  private sequenceNumber = 0;
  private pendingAcks = new Map<number, { resolve: Function; reject: Function }>();

  async connectToDoLake(env: Env): Promise<void> {
    const dolakeId = env.DOLAKE.idFromName('default');
    const dolakeStub = env.DOLAKE.get(dolakeId);

    // Establish WebSocket connection
    const response = await dolakeStub.fetch('https://dolake/ws', {
      headers: {
        'Upgrade': 'websocket',
        'X-Client-ID': this.ctx.id.toString(),
        'X-Shard-Name': this.shardName,
      },
    });

    if (response.status !== 101) {
      throw new Error('Failed to upgrade to WebSocket');
    }

    this.dolakeWs = response.webSocket!;
    this.dolakeWs.accept();

    // Handle incoming messages
    this.dolakeWs.addEventListener('message', (event) => {
      this.handleDoLakeMessage(JSON.parse(event.data as string));
    });

    // Handle connection close
    this.dolakeWs.addEventListener('close', (event) => {
      console.log(`DoLake connection closed: ${event.code} ${event.reason}`);
      this.dolakeWs = null;
      // Implement reconnection logic
    });

    // Send connect message with capabilities
    this.dolakeWs.send(JSON.stringify({
      type: 'connect',
      timestamp: Date.now(),
      sourceDoId: this.ctx.id.toString(),
      sourceShardName: this.shardName,
      lastAckSequence: this.lastAckSequence,
      protocolVersion: 1,
      capabilities: {
        binaryProtocol: false,
        compression: false,
        batching: true,
        maxBatchSize: 1000,
        maxMessageSize: 4 * 1024 * 1024,
      },
    }));
  }

  private handleDoLakeMessage(message: any): void {
    switch (message.type) {
      case 'ack':
        this.handleAck(message);
        break;
      case 'nack':
        this.handleNack(message);
        break;
      case 'status':
        this.handleStatus(message);
        break;
      case 'pong':
        // Heartbeat response
        break;
    }
  }
}
```

### Step 2: Capture CDC Events

Intercept database mutations to capture CDC events:

```typescript
class DoSQL implements DurableObject {
  private cdcBuffer: CDCEvent[] = [];
  private cdcSequence = 0;

  // Wrap your mutation operations
  async insert(table: string, data: Record<string, unknown>): Promise<void> {
    const rowId = data.id as string ?? crypto.randomUUID();

    // Perform the actual insert
    await this.db.insert(table, { ...data, id: rowId });

    // Capture CDC event
    this.captureCDCEvent({
      sequence: this.cdcSequence++,
      timestamp: Date.now(),
      operation: 'INSERT',
      table,
      rowId,
      after: data,
    });
  }

  async update(
    table: string,
    rowId: string,
    before: Record<string, unknown>,
    after: Record<string, unknown>
  ): Promise<void> {
    // Perform the actual update
    await this.db.update(table, rowId, after);

    // Capture CDC event
    this.captureCDCEvent({
      sequence: this.cdcSequence++,
      timestamp: Date.now(),
      operation: 'UPDATE',
      table,
      rowId,
      before,
      after,
    });
  }

  async delete(table: string, rowId: string, before: Record<string, unknown>): Promise<void> {
    // Perform the actual delete
    await this.db.delete(table, rowId);

    // Capture CDC event
    this.captureCDCEvent({
      sequence: this.cdcSequence++,
      timestamp: Date.now(),
      operation: 'DELETE',
      table,
      rowId,
      before,
    });
  }

  private captureCDCEvent(event: CDCEvent): void {
    this.cdcBuffer.push(event);

    // Flush buffer when it reaches threshold
    if (this.cdcBuffer.length >= 100) {
      this.flushCDCBuffer();
    }
  }
}
```

### Step 3: Send CDC Batches

Send batched CDC events to DoLake:

```typescript
class DoSQL implements DurableObject {
  private async flushCDCBuffer(): Promise<void> {
    if (this.cdcBuffer.length === 0 || !this.dolakeWs) {
      return;
    }

    const events = [...this.cdcBuffer];
    this.cdcBuffer = [];

    const sequenceNumber = this.sequenceNumber++;
    const message: CDCBatchMessage = {
      type: 'cdc_batch',
      timestamp: Date.now(),
      correlationId: crypto.randomUUID(),
      sourceDoId: this.ctx.id.toString(),
      sourceShardName: this.shardName,
      events,
      sequenceNumber,
      firstEventSequence: events[0].sequence,
      lastEventSequence: events[events.length - 1].sequence,
      sizeBytes: this.estimateSize(events),
      isRetry: false,
      retryCount: 0,
    };

    // Send and wait for ACK
    const ackPromise = this.waitForAck(sequenceNumber);
    this.dolakeWs.send(JSON.stringify(message));

    try {
      const ack = await ackPromise;
      this.lastAckSequence = sequenceNumber;
      await this.persistCheckpoint(sequenceNumber);
    } catch (error) {
      // Handle NACK or timeout - queue for retry
      this.queueForRetry(events, sequenceNumber);
    }
  }

  private waitForAck(sequenceNumber: number): Promise<AckMessage> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingAcks.delete(sequenceNumber);
        reject(new Error('ACK timeout'));
      }, 30000);

      this.pendingAcks.set(sequenceNumber, {
        resolve: (ack: AckMessage) => {
          clearTimeout(timeout);
          resolve(ack);
        },
        reject: (error: Error) => {
          clearTimeout(timeout);
          reject(error);
        },
      });
    });
  }

  private handleAck(message: AckMessage): void {
    const pending = this.pendingAcks.get(message.sequenceNumber);
    if (pending) {
      this.pendingAcks.delete(message.sequenceNumber);
      pending.resolve(message);
    }
  }

  private handleNack(message: any): void {
    const pending = this.pendingAcks.get(message.sequenceNumber);
    if (pending) {
      this.pendingAcks.delete(message.sequenceNumber);

      if (message.shouldRetry) {
        // Will be handled by rejection
        pending.reject(new Error(message.errorMessage));
      } else {
        // Permanent failure
        console.error('Permanent CDC failure:', message.errorMessage);
      }
    }
  }
}
```

### Step 4: Handle Retries

Implement retry logic for failed batches:

```typescript
class DoSQL implements DurableObject {
  private retryQueue: Array<{
    events: CDCEvent[];
    sequenceNumber: number;
    retryCount: number;
  }> = [];

  private queueForRetry(events: CDCEvent[], sequenceNumber: number): void {
    this.retryQueue.push({
      events,
      sequenceNumber,
      retryCount: 1,
    });

    // Schedule retry
    this.scheduleRetry();
  }

  private async processRetryQueue(): Promise<void> {
    if (!this.dolakeWs || this.retryQueue.length === 0) {
      return;
    }

    const item = this.retryQueue.shift()!;

    if (item.retryCount > 5) {
      // Max retries exceeded - store in dead letter queue
      await this.storeInDeadLetter(item.events);
      return;
    }

    const message: CDCBatchMessage = {
      type: 'cdc_batch',
      timestamp: Date.now(),
      correlationId: crypto.randomUUID(),
      sourceDoId: this.ctx.id.toString(),
      events: item.events,
      sequenceNumber: item.sequenceNumber,
      firstEventSequence: item.events[0].sequence,
      lastEventSequence: item.events[item.events.length - 1].sequence,
      sizeBytes: this.estimateSize(item.events),
      isRetry: true,
      retryCount: item.retryCount,
    };

    try {
      const ackPromise = this.waitForAck(item.sequenceNumber);
      this.dolakeWs.send(JSON.stringify(message));
      await ackPromise;
    } catch (error) {
      // Re-queue with incremented retry count
      this.retryQueue.push({
        ...item,
        retryCount: item.retryCount + 1,
      });
    }
  }
}
```

## Heartbeat and Keep-Alive

Implement heartbeats to maintain the connection:

```typescript
class DoSQL implements DurableObject {
  private heartbeatInterval: number | null = null;

  private startHeartbeat(): void {
    this.heartbeatInterval = setInterval(() => {
      if (this.dolakeWs?.readyState === WebSocket.OPEN) {
        this.dolakeWs.send(JSON.stringify({
          type: 'heartbeat',
          timestamp: Date.now(),
          sourceDoId: this.ctx.id.toString(),
          lastAckSequence: this.lastAckSequence,
          pendingEvents: this.cdcBuffer.length,
        }));
      }
    }, 30000) as unknown as number;
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }
}
```

## Graceful Shutdown

Handle shutdown by requesting a flush:

```typescript
class DoSQL implements DurableObject {
  async shutdown(): Promise<void> {
    if (this.cdcBuffer.length > 0) {
      await this.flushCDCBuffer();
    }

    if (this.dolakeWs?.readyState === WebSocket.OPEN) {
      // Request DoLake to flush before we disconnect
      this.dolakeWs.send(JSON.stringify({
        type: 'flush_request',
        timestamp: Date.now(),
        sourceDoId: this.ctx.id.toString(),
        reason: 'shutdown',
        correlationId: crypto.randomUUID(),
      }));

      // Give DoLake time to process
      await new Promise(resolve => setTimeout(resolve, 5000));

      this.dolakeWs.close(1000, 'Graceful shutdown');
    }
  }
}
```

## Complete Integration Example

Here's a complete example of a DoSQL class with DoLake integration:

```typescript
import {
  type CDCEvent,
  type CDCBatchMessage,
  type ConnectMessage,
  type HeartbeatMessage,
  type FlushRequestMessage,
  type AckMessage,
  type NackMessage,
} from '@dotdo/dolake';

interface DoSQLEnv {
  DOLAKE: DurableObjectNamespace;
}

export class DoSQL implements DurableObject {
  private ctx: DurableObjectState;
  private env: DoSQLEnv;

  // CDC state
  private dolakeWs: WebSocket | null = null;
  private cdcBuffer: CDCEvent[] = [];
  private cdcSequence = 0;
  private sequenceNumber = 0;
  private lastAckSequence = 0;
  private pendingAcks = new Map<number, { resolve: Function; reject: Function }>();
  private retryQueue: Array<{
    events: CDCEvent[];
    sequenceNumber: number;
    retryCount: number;
  }> = [];

  // Configuration
  private readonly shardName: string;
  private readonly batchSize = 100;
  private readonly batchTimeoutMs = 5000;

  constructor(ctx: DurableObjectState, env: DoSQLEnv) {
    this.ctx = ctx;
    this.env = env;
    this.shardName = `shard-${ctx.id.toString().slice(0, 8)}`;

    // Restore state
    this.ctx.blockConcurrencyWhile(async () => {
      this.cdcSequence = (await ctx.storage.get('cdc_sequence')) ?? 0;
      this.sequenceNumber = (await ctx.storage.get('batch_sequence')) ?? 0;
      this.lastAckSequence = (await ctx.storage.get('last_ack_sequence')) ?? 0;
    });

    // Connect to DoLake
    this.connectToDoLake();
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Handle your DoSQL API endpoints
    // ...

    return new Response('OK');
  }

  // ==========================================================================
  // DoLake Connection
  // ==========================================================================

  private async connectToDoLake(): Promise<void> {
    try {
      const dolakeId = this.env.DOLAKE.idFromName('default');
      const dolakeStub = this.env.DOLAKE.get(dolakeId);

      const response = await dolakeStub.fetch('https://dolake/ws', {
        headers: {
          'Upgrade': 'websocket',
          'X-Client-ID': this.ctx.id.toString(),
          'X-Shard-Name': this.shardName,
        },
      });

      if (response.status !== 101) {
        throw new Error(`Failed to upgrade: ${response.status}`);
      }

      this.dolakeWs = response.webSocket!;
      this.dolakeWs.accept();

      this.dolakeWs.addEventListener('message', (event) => {
        const message = JSON.parse(event.data as string);
        this.handleMessage(message);
      });

      this.dolakeWs.addEventListener('close', () => {
        this.dolakeWs = null;
        // Reconnect after delay
        setTimeout(() => this.connectToDoLake(), 5000);
      });

      // Send connect message
      const connectMsg: ConnectMessage = {
        type: 'connect',
        timestamp: Date.now(),
        sourceDoId: this.ctx.id.toString(),
        sourceShardName: this.shardName,
        lastAckSequence: this.lastAckSequence,
        protocolVersion: 1,
        capabilities: {
          binaryProtocol: false,
          compression: false,
          batching: true,
          maxBatchSize: 1000,
          maxMessageSize: 4 * 1024 * 1024,
        },
      };
      this.dolakeWs.send(JSON.stringify(connectMsg));

      // Start heartbeat
      this.startHeartbeat();

    } catch (error) {
      console.error('Failed to connect to DoLake:', error);
      setTimeout(() => this.connectToDoLake(), 5000);
    }
  }

  private handleMessage(message: any): void {
    switch (message.type) {
      case 'ack':
        this.handleAck(message as AckMessage);
        break;
      case 'nack':
        this.handleNack(message as NackMessage);
        break;
      case 'status':
        console.log('DoLake status:', message);
        break;
    }
  }

  private handleAck(ack: AckMessage): void {
    const pending = this.pendingAcks.get(ack.sequenceNumber);
    if (pending) {
      this.pendingAcks.delete(ack.sequenceNumber);
      pending.resolve(ack);
    }
  }

  private handleNack(nack: NackMessage): void {
    const pending = this.pendingAcks.get(nack.sequenceNumber);
    if (pending) {
      this.pendingAcks.delete(nack.sequenceNumber);
      pending.reject(new Error(nack.errorMessage));
    }
  }

  // ==========================================================================
  // CDC Capture
  // ==========================================================================

  private captureCDC(event: Omit<CDCEvent, 'sequence' | 'timestamp'>): void {
    const fullEvent: CDCEvent = {
      ...event,
      sequence: this.cdcSequence++,
      timestamp: Date.now(),
    };

    this.cdcBuffer.push(fullEvent);

    if (this.cdcBuffer.length >= this.batchSize) {
      this.flushCDCBuffer();
    }
  }

  private async flushCDCBuffer(): Promise<void> {
    if (this.cdcBuffer.length === 0 || !this.dolakeWs) {
      return;
    }

    const events = [...this.cdcBuffer];
    this.cdcBuffer = [];

    const seq = this.sequenceNumber++;
    await this.ctx.storage.put('batch_sequence', this.sequenceNumber);

    const message: CDCBatchMessage = {
      type: 'cdc_batch',
      timestamp: Date.now(),
      correlationId: crypto.randomUUID(),
      sourceDoId: this.ctx.id.toString(),
      sourceShardName: this.shardName,
      events,
      sequenceNumber: seq,
      firstEventSequence: events[0].sequence,
      lastEventSequence: events[events.length - 1].sequence,
      sizeBytes: JSON.stringify(events).length,
      isRetry: false,
      retryCount: 0,
    };

    const ackPromise = new Promise<AckMessage>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingAcks.delete(seq);
        reject(new Error('ACK timeout'));
      }, 30000);

      this.pendingAcks.set(seq, {
        resolve: (ack: AckMessage) => {
          clearTimeout(timeout);
          resolve(ack);
        },
        reject: (err: Error) => {
          clearTimeout(timeout);
          reject(err);
        },
      });
    });

    this.dolakeWs.send(JSON.stringify(message));

    try {
      await ackPromise;
      this.lastAckSequence = seq;
      await this.ctx.storage.put('last_ack_sequence', seq);
    } catch (error) {
      // Queue for retry
      this.retryQueue.push({ events, sequenceNumber: seq, retryCount: 1 });
    }
  }

  // ==========================================================================
  // Heartbeat
  // ==========================================================================

  private heartbeatHandle: number | null = null;

  private startHeartbeat(): void {
    this.heartbeatHandle = setInterval(() => {
      if (this.dolakeWs?.readyState === WebSocket.OPEN) {
        const heartbeat: HeartbeatMessage = {
          type: 'heartbeat',
          timestamp: Date.now(),
          sourceDoId: this.ctx.id.toString(),
          lastAckSequence: this.lastAckSequence,
          pendingEvents: this.cdcBuffer.length,
        };
        this.dolakeWs.send(JSON.stringify(heartbeat));
      }
    }, 30000) as unknown as number;
  }
}
```

## Monitoring Integration

### Check Connection Status

```typescript
// In your DoSQL class
async getDoLakeStatus(): Promise<any> {
  if (!this.dolakeWs) {
    return { connected: false };
  }

  return {
    connected: this.dolakeWs.readyState === WebSocket.OPEN,
    lastAckSequence: this.lastAckSequence,
    pendingBatches: this.pendingAcks.size,
    retryQueueLength: this.retryQueue.length,
    bufferSize: this.cdcBuffer.length,
  };
}
```

### Expose Metrics

```typescript
async getMetrics(): Promise<string> {
  const status = await this.getDoLakeStatus();

  return `
# HELP dosql_dolake_connected DoLake connection status
# TYPE dosql_dolake_connected gauge
dosql_dolake_connected ${status.connected ? 1 : 0}

# HELP dosql_cdc_buffer_size Current CDC buffer size
# TYPE dosql_cdc_buffer_size gauge
dosql_cdc_buffer_size ${status.bufferSize}

# HELP dosql_pending_acks Number of pending ACKs
# TYPE dosql_pending_acks gauge
dosql_pending_acks ${status.pendingBatches}

# HELP dosql_retry_queue_length Retry queue length
# TYPE dosql_retry_queue_length gauge
dosql_retry_queue_length ${status.retryQueueLength}
`.trim();
}
```

## Troubleshooting

### Connection Issues

1. **WebSocket upgrade fails**
   - Check that DoLake DO is properly exported
   - Verify R2 bucket binding exists
   - Check for wrangler configuration errors

2. **Frequent disconnections**
   - Increase heartbeat interval
   - Check for network issues
   - Review DoLake logs for errors

### ACK/NACK Issues

1. **Timeout waiting for ACK**
   - DoLake may be under heavy load
   - Check DoLake buffer utilization via `/status`
   - Consider reducing batch size

2. **Receiving NACKs with `buffer_full`**
   - DoLake buffer is at capacity
   - Wait and retry with backoff
   - Consider multiple DoLake instances

### Data Consistency

1. **Missing events in Iceberg**
   - Check for events in retry queue
   - Verify lastAckSequence progression
   - Review dead letter storage

2. **Duplicate events**
   - DoLake handles deduplication automatically
   - Ensure sequence numbers are monotonic
   - Check for connection reconnection issues
