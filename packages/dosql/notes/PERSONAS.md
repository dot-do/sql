# DoSQL User Personas

This document defines the target user personas for DoSQL, helping guide product decisions, documentation priorities, and feature development.

---

## Table of Contents

- [Overview](#overview)
- [Persona 1: SaaS Developer](#persona-1-saas-developer)
- [Persona 2: Real-time App Developer](#persona-2-real-time-app-developer)
- [Persona 3: Edge Analytics Developer](#persona-3-edge-analytics-developer)
- [Persona 4: E-commerce Developer](#persona-4-e-commerce-developer)
- [Persona 5: IoT Developer](#persona-5-iot-developer)
- [Persona Summary Matrix](#persona-summary-matrix)

---

## Overview

DoSQL targets developers building applications on Cloudflare Workers and Durable Objects who need:

- Type-safe database access without WASM overhead
- Multi-tenant data isolation at the edge
- Real-time data capabilities with CDC streaming
- Time travel and audit trail functionality
- Small bundle size within Cloudflare's 1MB limit

---

## Persona 1: SaaS Developer

### Profile

| Attribute | Description |
|-----------|-------------|
| **Name** | Sarah Chen |
| **Role** | Full-Stack Engineer at a B2B SaaS startup |
| **Experience** | 5 years TypeScript/Node.js, 2 years Cloudflare Workers |
| **Team Size** | 4-8 engineers |
| **Company Stage** | Series A, 200+ customers |

### Background

Sarah is building a multi-tenant project management SaaS where each customer organization needs isolated data. She previously used a managed PostgreSQL database with connection pooling, but faced cold start latency issues and scaling challenges with their global customer base.

### Goals

1. **Tenant Isolation**: Complete data separation between customers without complex schemas
2. **Global Performance**: Sub-100ms response times regardless of user location
3. **Type Safety**: Catch SQL errors at compile time, not runtime
4. **Cost Efficiency**: Pay per request, not for idle database connections

### Pain Points

- Connection pool exhaustion during traffic spikes
- Complex tenant ID filtering in every query
- Cold start latency affecting user experience
- Managing database migrations across 200+ tenant schemas

### Technical Environment

```
Frontend:     React/Next.js
Backend:      Cloudflare Workers
Current DB:   PostgreSQL (Neon/PlanetScale)
Auth:         Clerk or Auth0
```

### Why DoSQL Fits

```
+------------------------------------------+
|           SaaS MULTI-TENANCY             |
|                                          |
|   +---------+   +---------+   +-------+  |
|   | Tenant1 |   | Tenant2 |   | ...   |  |
|   |   DO    |   |   DO    |   |  DO   |  |
|   +---------+   +---------+   +-------+  |
|       |             |             |      |
|       +-------------+-------------+      |
|                     |                    |
|              +------v------+             |
|              |   R2 Cold   |             |
|              |   Storage   |             |
|              +-------------+             |
+------------------------------------------+

- Each tenant = isolated Durable Object
- No tenant ID filtering needed
- Automatic data isolation
- Hot/cold tiering reduces DO costs
```

### Key Features Used

- **Multi-tenant isolation** via separate DOs per tenant
- **Auto-migrations** for schema updates across all tenants
- **Type-safe SQL** to prevent runtime query errors
- **CDC streaming** to sync changes to analytics lakehouse

### Success Metrics

- < 50ms p99 latency for CRUD operations
- Zero cross-tenant data leaks
- 80% reduction in database costs vs managed PostgreSQL

### Sample Code

```typescript
// src/tenant-database.ts
import { DB } from 'dosql';

export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private async getDB() {
    if (!this.db) {
      this.db = await DB('tenant', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.state.storage,
          cold: this.env.TENANT_DATA_BUCKET,
        },
      });
    }
    return this.db;
  }

  async createProject(name: string, ownerId: number) {
    const db = await this.getDB();
    return db.run(`
      INSERT INTO projects (name, owner_id, created_at)
      VALUES (?, ?, CURRENT_TIMESTAMP)
    `, [name, ownerId]);
  }

  async getProjects(ownerId: number) {
    const db = await this.getDB();
    return db.query<{ id: number; name: string; created_at: string }>(`
      SELECT id, name, created_at
      FROM projects
      WHERE owner_id = ?
      ORDER BY created_at DESC
    `, [ownerId]);
  }
}
```

---

## Persona 2: Real-time App Developer

### Profile

| Attribute | Description |
|-----------|-------------|
| **Name** | Marcus Johnson |
| **Role** | Lead Developer at a collaboration tools company |
| **Experience** | 7 years JavaScript, 3 years building real-time apps |
| **Team Size** | 10-15 engineers |
| **Company Stage** | Series B, building Notion competitor |

### Background

Marcus is building a real-time collaborative document editor. Users need to see each other's changes instantly, with full offline support and conflict resolution. The app needs to handle thousands of concurrent editors while maintaining sub-second sync.

### Goals

1. **Real-time Sync**: Changes visible to all collaborators within 100ms
2. **Offline Support**: Full functionality without network, sync when reconnected
3. **Conflict Resolution**: Graceful handling of concurrent edits
4. **Audit Trail**: Complete history of who changed what and when

### Pain Points

- WebSocket server scaling complexity
- Operational transforms are hard to implement correctly
- Lost updates during network partitions
- No visibility into data change history

### Technical Environment

```
Frontend:     React with custom CRDT library
Backend:      Cloudflare Workers + Durable Objects
Real-time:    WebSockets via DO hibernation
State:        Custom in-memory state machine
```

### Why DoSQL Fits

```
+----------------------------------------------------------+
|                  REAL-TIME COLLABORATION                   |
|                                                            |
|     User A          Document DO           User B           |
|    +------+        +-----------+         +------+          |
|    |Editor|<======>|  DoSQL    |<=======>|Editor|          |
|    +------+  WS    |           |   WS    +------+          |
|                    |  +-----+  |                           |
|                    |  | WAL |--|---> CDC Stream            |
|                    |  +-----+  |            |              |
|                    +-----------+            v              |
|                                      +------------+        |
|                                      | Lakehouse  |        |
|                                      | (History)  |        |
|                                      +------------+        |
+----------------------------------------------------------+

- WAL captures every change
- CDC streams to lakehouse for analytics
- Time travel enables "undo" and version history
- Branching supports "what-if" scenarios
```

### Key Features Used

- **WAL-based CDC** for real-time change streaming
- **Time travel** for version history and undo
- **Branching** for draft/published workflows
- **WebSocket RPC** for low-latency updates

### Success Metrics

- < 100ms latency for change propagation
- 99.9% sync success rate
- Complete audit trail for compliance

### Sample Code

```typescript
// src/document-do.ts
import { DB, createCDC } from 'dosql';

export class DocumentDO implements DurableObject {
  private db: Database | null = null;
  private connections = new Map<WebSocket, { userId: string }>();

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {
    this.state.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('ping', 'pong')
    );
  }

  async fetch(request: Request) {
    const url = new URL(request.url);

    if (url.pathname === '/ws') {
      const [client, server] = Object.values(new WebSocketPair());
      const userId = url.searchParams.get('userId')!;

      this.state.acceptWebSocket(server);
      this.connections.set(server, { userId });

      // Send current document state
      const doc = await this.getDocument();
      server.send(JSON.stringify({ type: 'init', data: doc }));

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response('Not found', { status: 404 });
  }

  async webSocketMessage(ws: WebSocket, message: string) {
    const { type, data } = JSON.parse(message);
    const db = await this.getDB();
    const { userId } = this.connections.get(ws)!;

    if (type === 'update') {
      // Record change with user attribution
      await db.run(`
        INSERT INTO changes (user_id, path, operation, value, created_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      `, [userId, data.path, data.op, JSON.stringify(data.value)]);

      // Broadcast to all other connections
      const update = JSON.stringify({ type: 'update', data, userId });
      for (const [socket] of this.connections) {
        if (socket !== ws && socket.readyState === WebSocket.READY_STATE_OPEN) {
          socket.send(update);
        }
      }
    }
  }

  async getVersionHistory(limit = 50) {
    const db = await this.getDB();
    return db.query(`
      SELECT c.*, u.name as user_name
      FROM changes c
      JOIN users u ON c.user_id = u.id
      ORDER BY c.created_at DESC
      LIMIT ?
    `, [limit]);
  }

  async restoreVersion(timestamp: string) {
    const db = await this.getDB();
    // Time travel query to get state at timestamp
    return db.query(`
      SELECT * FROM document_state
      FOR SYSTEM_TIME AS OF TIMESTAMP ?
    `, [timestamp]);
  }
}
```

---

## Persona 3: Edge Analytics Developer

### Profile

| Attribute | Description |
|-----------|-------------|
| **Name** | Priya Sharma |
| **Role** | Data Engineer at a product analytics company |
| **Experience** | 6 years data engineering, 2 years edge computing |
| **Team Size** | 5-person data team within 50-person company |
| **Company Stage** | Growth stage, processing 10B+ events/month |

### Background

Priya is building a privacy-first analytics platform that processes user events at the edge before aggregating to a central data warehouse. GDPR and data residency requirements mean user data must be processed within geographic boundaries before any aggregation.

### Goals

1. **Data Residency**: Process data in the region it was generated
2. **Real-time Aggregations**: Pre-aggregate metrics at the edge
3. **Privacy Compliance**: PII never leaves the origin region
4. **Cost Efficiency**: Reduce central data warehouse query costs

### Pain Points

- Cross-region data transfer costs and latency
- Complex data residency compliance
- Central warehouse overwhelmed by raw event volume
- Slow batch processing delays insights by hours

### Technical Environment

```
Ingestion:    Cloudflare Workers (global)
Processing:   Durable Objects (regional)
Warehouse:    Snowflake / BigQuery
Streaming:    Kafka via Cloudflare Queues
```

### Why DoSQL Fits

```
+------------------------------------------------------------------+
|                    EDGE ANALYTICS PIPELINE                         |
|                                                                    |
|   US Region              EU Region              APAC Region        |
|  +-----------+          +-----------+          +-----------+       |
|  |  US Edge  |          |  EU Edge  |          | APAC Edge |       |
|  |   DO's    |          |   DO's    |          |   DO's    |       |
|  +-----+-----+          +-----+-----+          +-----+-----+       |
|        |                      |                      |             |
|        | CDC                  | CDC                  | CDC         |
|        v                      v                      v             |
|  +-----+-----+          +-----+-----+          +-----+-----+       |
|  |US Regional|          |EU Regional|          |APAC Regional|    |
|  | Aggregator|          | Aggregator|          | Aggregator |     |
|  +-----+-----+          +-----+-----+          +-----+-----+       |
|        |                      |                      |             |
|        +----------------------+----------------------+             |
|                               |                                    |
|                        +------v-------+                            |
|                        | Central DWH  |                            |
|                        | (Aggregated) |                            |
|                        +--------------+                            |
+------------------------------------------------------------------+

- PII processed and stripped at edge
- Only aggregates sent to central warehouse
- CDC enables real-time streaming
- Regional compliance guaranteed
```

### Key Features Used

- **CDC streaming** to regional aggregators
- **Virtual tables** to query R2 Parquet files
- **Time travel** for point-in-time reporting
- **Tiered storage** for hot analytics, cold archive

### Success Metrics

- 100% data residency compliance
- < 5 second latency for regional metrics
- 70% reduction in warehouse query costs

### Sample Code

```typescript
// src/edge-analytics.ts
import { DB, createCDC } from 'dosql';

export class EdgeAnalyticsDO implements DurableObject {
  private db: Database | null = null;
  private cdc: CDC | null = null;

  async ingestEvent(event: AnalyticsEvent) {
    const db = await this.getDB();

    // Store raw event with PII
    await db.run(`
      INSERT INTO raw_events (
        session_id, user_id, event_type,
        properties, timestamp, ip_address
      ) VALUES (?, ?, ?, ?, ?, ?)
    `, [
      event.sessionId,
      event.userId,
      event.type,
      JSON.stringify(event.properties),
      event.timestamp,
      event.ipAddress,  // PII - stays in region
    ]);

    // Update real-time aggregates (no PII)
    await db.run(`
      INSERT INTO hourly_metrics (
        hour, event_type, count, unique_sessions
      ) VALUES (
        strftime('%Y-%m-%d %H:00:00', ?),
        ?, 1, 1
      )
      ON CONFLICT (hour, event_type) DO UPDATE SET
        count = count + 1,
        unique_sessions = unique_sessions +
          CASE WHEN NOT EXISTS (
            SELECT 1 FROM session_seen
            WHERE session_id = ? AND hour = strftime('%Y-%m-%d %H:00:00', ?)
          ) THEN 1 ELSE 0 END
    `, [event.timestamp, event.type, event.sessionId, event.timestamp]);
  }

  async setupCDCStreaming(env: Env) {
    const db = await this.getDB();
    this.cdc = createCDC(db.backend);

    // Stream aggregated metrics to central warehouse
    for await (const change of this.cdc.subscribe(0n)) {
      if (change.table === 'hourly_metrics') {
        // Only aggregates leave the region
        await env.ANALYTICS_QUEUE.send({
          region: this.getRegion(),
          table: 'hourly_metrics',
          data: change.after,  // No PII in aggregates
        });
      }
    }
  }

  async queryRegionalMetrics(startTime: string, endTime: string) {
    const db = await this.getDB();

    return db.query<{
      event_type: string;
      total_count: number;
      unique_sessions: number;
    }>(`
      SELECT
        event_type,
        SUM(count) as total_count,
        SUM(unique_sessions) as unique_sessions
      FROM hourly_metrics
      WHERE hour BETWEEN ? AND ?
      GROUP BY event_type
      ORDER BY total_count DESC
    `, [startTime, endTime]);
  }

  // Query archived data from R2 using virtual tables
  async queryHistoricalMetrics(date: string) {
    const db = await this.getDB();

    return db.query(`
      SELECT event_type, SUM(count) as total
      FROM 'r2://analytics-archive/metrics/${date}/*.parquet'
      GROUP BY event_type
    `);
  }
}
```

---

## Persona 4: E-commerce Developer

### Profile

| Attribute | Description |
|-----------|-------------|
| **Name** | Alex Rivera |
| **Role** | Senior Backend Engineer at a D2C e-commerce platform |
| **Experience** | 8 years backend development, 4 years e-commerce |
| **Team Size** | 20+ engineers across multiple squads |
| **Company Stage** | Late stage, $50M+ annual GMV |

### Background

Alex is responsible for the order management and inventory systems of a direct-to-consumer brand. The platform needs to handle flash sales with 100x traffic spikes while maintaining accurate inventory counts and preventing overselling.

### Goals

1. **Inventory Accuracy**: Never oversell, even during flash sales
2. **High Availability**: Zero downtime during peak events
3. **Performance**: Sub-200ms checkout latency
4. **Auditability**: Complete order history for customer service

### Pain Points

- Race conditions during concurrent inventory updates
- Database locks causing checkout timeouts during sales
- Inconsistent inventory counts across services
- Difficult to debug order issues without complete history

### Technical Environment

```
Frontend:     Headless storefront (Shopify Hydrogen)
Backend:      Cloudflare Workers
Payments:     Stripe
Inventory:    Currently PostgreSQL + Redis
Fulfillment:  Third-party WMS integration
```

### Why DoSQL Fits

```
+------------------------------------------------------------------+
|                    E-COMMERCE ARCHITECTURE                         |
|                                                                    |
|  +-----------+     +------------+     +-----------+                |
|  | Product   |     |  Cart DO   |     | Order DO  |                |
|  | Catalog   |     | (per user) |     | (per txn) |                |
|  |    DO     |     +------+-----+     +-----+-----+                |
|  +-----+-----+            |                 |                      |
|        |                  |                 |                      |
|        v                  v                 v                      |
|  +----------------------------------------------------------+     |
|  |              Inventory Coordinator DO                      |    |
|  |  +--------+  +--------+  +--------+  +--------+           |    |
|  |  | SKU A  |  | SKU B  |  | SKU C  |  | SKU D  |           |    |
|  |  | DO     |  | DO     |  | DO     |  | DO     |           |    |
|  |  +--------+  +--------+  +--------+  +--------+           |    |
|  +----------------------------------------------------------+     |
|                              |                                     |
|                              | CDC                                 |
|                              v                                     |
|                    +------------------+                            |
|                    | Order Analytics  |                            |
|                    | (R2 Lakehouse)   |                            |
|                    +------------------+                            |
+------------------------------------------------------------------+

- One DO per SKU = no contention
- Strong consistency via DO single-threading
- Time travel for order history
- CDC streams to analytics
```

### Key Features Used

- **Single-threaded DOs** for race-free inventory
- **Transactions** for atomic order creation
- **Time travel** for order state history
- **CDC** for real-time inventory analytics

### Success Metrics

- Zero oversells during flash sales
- < 200ms p99 checkout latency
- 100% order auditability

### Sample Code

```typescript
// src/inventory-sku.ts
import { DB } from 'dosql';

export class InventorySKU implements DurableObject {
  private db: Database | null = null;

  async reserve(orderId: string, quantity: number): Promise<ReserveResult> {
    const db = await this.getDB();

    // Single-threaded execution = no race conditions
    return db.transaction(async (tx) => {
      // Check current availability
      const [stock] = await tx.query<{ available: number; reserved: number }>(`
        SELECT available, reserved FROM inventory LIMIT 1
      `);

      if (stock.available < quantity) {
        return {
          success: false,
          error: 'INSUFFICIENT_STOCK',
          available: stock.available,
        };
      }

      // Create reservation
      await tx.run(`
        INSERT INTO reservations (order_id, quantity, status, created_at, expires_at)
        VALUES (?, ?, 'PENDING', CURRENT_TIMESTAMP, datetime('now', '+15 minutes'))
      `, [orderId, quantity]);

      // Update inventory
      await tx.run(`
        UPDATE inventory
        SET available = available - ?,
            reserved = reserved + ?
      `, [quantity, quantity]);

      return {
        success: true,
        reservationId: orderId,
        expiresAt: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
      };
    });
  }

  async confirmReservation(orderId: string): Promise<void> {
    const db = await this.getDB();

    await db.transaction(async (tx) => {
      const [reservation] = await tx.query<{ quantity: number; status: string }>(`
        SELECT quantity, status FROM reservations WHERE order_id = ?
      `, [orderId]);

      if (!reservation || reservation.status !== 'PENDING') {
        throw new Error('INVALID_RESERVATION');
      }

      // Mark reservation as confirmed
      await tx.run(`
        UPDATE reservations SET status = 'CONFIRMED' WHERE order_id = ?
      `, [orderId]);

      // Move from reserved to sold
      await tx.run(`
        UPDATE inventory
        SET reserved = reserved - ?,
            sold = sold + ?
      `, [reservation.quantity, reservation.quantity]);
    });
  }

  async releaseExpiredReservations(): Promise<number> {
    const db = await this.getDB();

    return db.transaction(async (tx) => {
      // Find expired reservations
      const expired = await tx.query<{ order_id: string; quantity: number }>(`
        SELECT order_id, quantity FROM reservations
        WHERE status = 'PENDING' AND expires_at < CURRENT_TIMESTAMP
      `);

      if (expired.length === 0) return 0;

      // Release inventory
      const totalQuantity = expired.reduce((sum, r) => sum + r.quantity, 0);
      await tx.run(`
        UPDATE inventory
        SET available = available + ?,
            reserved = reserved - ?
      `, [totalQuantity, totalQuantity]);

      // Mark as expired
      await tx.run(`
        UPDATE reservations SET status = 'EXPIRED'
        WHERE status = 'PENDING' AND expires_at < CURRENT_TIMESTAMP
      `);

      return expired.length;
    });
  }

  // Time travel for order debugging
  async getInventoryStateAt(timestamp: string) {
    const db = await this.getDB();
    return db.query(`
      SELECT * FROM inventory
      FOR SYSTEM_TIME AS OF TIMESTAMP ?
    `, [timestamp]);
  }
}
```

---

## Persona 5: IoT Developer

### Profile

| Attribute | Description |
|-----------|-------------|
| **Name** | James Park |
| **Role** | IoT Platform Architect at a smart building company |
| **Experience** | 10 years embedded systems, 5 years cloud IoT |
| **Team Size** | 8 engineers (firmware + cloud) |
| **Company Stage** | Series B, 500+ building deployments |

### Background

James is building the data platform for a smart building management system. Thousands of sensors per building generate telemetry data that needs local processing for real-time alerts while also being aggregated for building-wide analytics and historical reporting.

### Goals

1. **Local Processing**: Process sensor data at the edge for real-time alerts
2. **Scalability**: Handle millions of sensor readings per building per day
3. **Offline Resilience**: Continue operating when cloud connectivity is lost
4. **Historical Analysis**: Query weeks of sensor data for trend analysis

### Pain Points

- High cloud egress costs for raw telemetry
- Central database overwhelmed by sensor volume
- Alerting delays when processing remotely
- No local data when cloud is unreachable

### Technical Environment

```
Devices:      ESP32 sensors, Raspberry Pi gateways
Edge:         Cloudflare Workers (regional)
Protocol:     MQTT -> WebSocket bridge
Storage:      Currently InfluxDB Cloud
Alerts:       PagerDuty integration
```

### Why DoSQL Fits

```
+------------------------------------------------------------------+
|                    IOT EDGE ARCHITECTURE                           |
|                                                                    |
|  Building A               Building B               Building C      |
|  +----------+             +----------+             +----------+    |
|  | Sensors  |             | Sensors  |             | Sensors  |    |
|  +----+-----+             +----+-----+             +----+-----+    |
|       |                        |                        |          |
|       v                        v                        v          |
|  +----+-----+             +----+-----+             +----+-----+    |
|  |Building A|             |Building B|             |Building C|    |
|  |    DO    |             |    DO    |             |    DO    |    |
|  |          |             |          |             |          |    |
|  | +------+ |             | +------+ |             | +------+ |    |
|  | |DoSQL | |             | |DoSQL | |             | |DoSQL | |    |
|  | +------+ |             | +------+ |             | +------+ |    |
|  +----+-----+             +----+-----+             +----+-----+    |
|       |                        |                        |          |
|       | Rollups                | Rollups                | Rollups  |
|       v                        v                        v          |
|  +----------------------------------------------------------+     |
|  |                    Central Analytics DO                    |    |
|  |  (Aggregated metrics from all buildings)                  |    |
|  +----------------------------------------------------------+     |
+------------------------------------------------------------------+

- Per-building DO for local processing
- Real-time alerting at edge
- Only rollups sent to central
- Hot tier for recent readings
- R2 cold tier for history
```

### Key Features Used

- **Hot/cold tiering** for sensor data lifecycle
- **Real-time queries** for alerting rules
- **Time travel** for anomaly investigation
- **Virtual tables** to query archived Parquet data

### Success Metrics

- < 1 second alert latency for threshold breaches
- 90% reduction in cloud egress costs
- 30+ days of queryable history per building

### Sample Code

```typescript
// src/building-telemetry.ts
import { DB } from 'dosql';

interface SensorReading {
  sensorId: string;
  type: 'temperature' | 'humidity' | 'co2' | 'occupancy';
  value: number;
  timestamp: number;
}

interface Alert {
  sensorId: string;
  type: string;
  value: number;
  threshold: number;
  timestamp: string;
}

export class BuildingTelemetryDO implements DurableObject {
  private db: Database | null = null;
  private alertCallbacks: ((alert: Alert) => void)[] = [];

  async ingestReading(reading: SensorReading): Promise<Alert | null> {
    const db = await this.getDB();

    // Store reading
    await db.run(`
      INSERT INTO readings (sensor_id, type, value, timestamp)
      VALUES (?, ?, ?, ?)
    `, [reading.sensorId, reading.type, reading.value, reading.timestamp]);

    // Update running aggregates (1-minute buckets)
    const bucket = Math.floor(reading.timestamp / 60000) * 60000;
    await db.run(`
      INSERT INTO minute_aggregates (sensor_id, type, bucket, min, max, sum, count)
      VALUES (?, ?, ?, ?, ?, ?, 1)
      ON CONFLICT (sensor_id, type, bucket) DO UPDATE SET
        min = MIN(min, excluded.min),
        max = MAX(max, excluded.max),
        sum = sum + excluded.sum,
        count = count + 1
    `, [reading.sensorId, reading.type, bucket, reading.value, reading.value, reading.value]);

    // Check alert thresholds
    return this.checkAlerts(reading);
  }

  private async checkAlerts(reading: SensorReading): Promise<Alert | null> {
    const db = await this.getDB();

    // Get threshold for this sensor type
    const [threshold] = await db.query<{
      min_value: number | null;
      max_value: number | null;
    }>(`
      SELECT min_value, max_value
      FROM alert_thresholds
      WHERE sensor_id = ? AND type = ?
    `, [reading.sensorId, reading.type]);

    if (!threshold) return null;

    let alert: Alert | null = null;

    if (threshold.max_value !== null && reading.value > threshold.max_value) {
      alert = {
        sensorId: reading.sensorId,
        type: `${reading.type}_HIGH`,
        value: reading.value,
        threshold: threshold.max_value,
        timestamp: new Date(reading.timestamp).toISOString(),
      };
    } else if (threshold.min_value !== null && reading.value < threshold.min_value) {
      alert = {
        sensorId: reading.sensorId,
        type: `${reading.type}_LOW`,
        value: reading.value,
        threshold: threshold.min_value,
        timestamp: new Date(reading.timestamp).toISOString(),
      };
    }

    if (alert) {
      // Record alert
      await db.run(`
        INSERT INTO alerts (sensor_id, alert_type, value, threshold, timestamp)
        VALUES (?, ?, ?, ?, ?)
      `, [alert.sensorId, alert.type, alert.value, alert.threshold, alert.timestamp]);

      // Notify callbacks
      this.alertCallbacks.forEach(cb => cb(alert!));
    }

    return alert;
  }

  async getHourlyAverages(sensorId: string, type: string, hours: number = 24) {
    const db = await this.getDB();
    const cutoff = Date.now() - hours * 60 * 60 * 1000;

    return db.query<{ hour: number; avg_value: number; min_value: number; max_value: number }>(`
      SELECT
        (bucket / 3600000) * 3600000 as hour,
        SUM(sum) / SUM(count) as avg_value,
        MIN(min) as min_value,
        MAX(max) as max_value
      FROM minute_aggregates
      WHERE sensor_id = ? AND type = ? AND bucket >= ?
      GROUP BY hour
      ORDER BY hour DESC
    `, [sensorId, type, cutoff]);
  }

  // Query historical data from R2 archive
  async getHistoricalData(sensorId: string, startDate: string, endDate: string) {
    const db = await this.getDB();

    return db.query(`
      SELECT timestamp, value
      FROM 'r2://building-archive/${this.buildingId}/readings/*.parquet'
      WHERE sensor_id = ?
        AND date BETWEEN ? AND ?
      ORDER BY timestamp
    `, [sensorId, startDate, endDate]);
  }

  // Investigate anomaly using time travel
  async investigateAnomaly(sensorId: string, anomalyTime: string) {
    const db = await this.getDB();

    // Get readings around the anomaly
    const before = new Date(new Date(anomalyTime).getTime() - 30 * 60 * 1000).toISOString();
    const after = new Date(new Date(anomalyTime).getTime() + 30 * 60 * 1000).toISOString();

    return db.query(`
      SELECT * FROM readings
      WHERE sensor_id = ? AND timestamp BETWEEN ? AND ?
      ORDER BY timestamp
    `, [sensorId, before, after]);
  }
}
```

---

## Persona Summary Matrix

| Persona | Primary Use Case | Key Features | Performance Target |
|---------|-----------------|--------------|-------------------|
| **SaaS Developer** | Multi-tenant isolation | Auto-migrations, Type safety | < 50ms p99 |
| **Real-time App** | Collaborative editing | CDC, Time travel, WebSocket RPC | < 100ms sync |
| **Edge Analytics** | Privacy-first processing | CDC streaming, Virtual tables | < 5s regional |
| **E-commerce** | Inventory management | Transactions, Single-threading | < 200ms checkout |
| **IoT Developer** | Sensor telemetry | Hot/cold tiering, Aggregations | < 1s alerting |

### Feature Adoption by Persona

| Feature | SaaS | Real-time | Analytics | E-commerce | IoT |
|---------|------|-----------|-----------|------------|-----|
| Type-safe SQL | Yes | Yes | Yes | Yes | Yes |
| Auto-migrations | Yes | Yes | Moderate | Yes | Moderate |
| Transactions | Moderate | Yes | Low | Critical | Moderate |
| Time Travel | Moderate | Critical | Yes | Yes | Yes |
| Branching | Low | Yes | Low | Low | Low |
| CDC Streaming | Yes | Critical | Critical | Yes | Yes |
| Virtual Tables | Low | Low | Critical | Low | Yes |
| Hot/Cold Tiering | Yes | Moderate | Yes | Moderate | Critical |
| WebSocket RPC | Low | Critical | Low | Moderate | Moderate |

---

## Next Steps

See [USE_CASES.md](./USE_CASES.md) for detailed canonical use cases with architecture diagrams and implementation patterns for each persona.
