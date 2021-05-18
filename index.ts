import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, PluginEvent, PluginJobs } from '@posthog/plugin-scaffold'
import { Pool } from 'pg'


type RedshiftMeta = PluginMeta<{
    global: {
        pgPool: Pool
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
        sanitizedTableName: string
    }
    config: {
        clusterHost: string
        clusterPort: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        uploadMinutes: string
        uploadMegabytes: string
        eventsToIgnore: string
    }
}>

type RedshiftPlugin = Plugin<RedshiftMeta>

interface UploadJobPayload {
    batch: PluginEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

export const jobs: PluginJobs<RedshiftMeta> = {
    uploadBatchToRedshift: async (payload: UploadJobPayload, meta: RedshiftMeta) => {
        // insertBatchIntoRedshift(payload, meta)
    },
}

export const setupPlugin: RedshiftPlugin['setupPlugin'] = (meta) => {
    const { global, config } = meta

    // Max Redshift insert is 16 MB: https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 10))
    const uploadMinutes = Math.max(1, Math.min(parseInt(config.uploadMinutes) || 1, 60))

    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Config option ${option} is missing!`)
        }
    }

    global.sanitizedTableName = sanitizeSqlIdentifier(config.tableName)

    global.pgPool = new Pool({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
        max: 2
    })

    global.pgPool.query(
        `CREATE TABLE IF NOT EXISTS public.${global.sanitizedTableName} (
            uuid character varying(200),
            event character varying(200),
            properties varchar,
            elements varchar,
            set varchar,
            set_once varchar,
            "timestamp" timestamp with time zone,
            team_id integer,
            distinct_id character varying(200),
            ip character varying(50),
            created_at timestamp with time zone,
            site_url character varying(200)
        );`, 
        (err: Error) => {
            if (err) {
                throw new Error(`Unable to connect to Redshift cluster with error: ${err}`)
            }
        }
    )

    global.buffer = createBuffer({
        limit: uploadMegabytes * 1024 * 1024,
        timeoutSeconds: uploadMinutes * 60,
        onFlush: async (batch) => {
            sendBatchToRedshift({ batch, batchId: Math.floor(Math.random() * 1000000), retriesPerformedSoFar: 0 }, meta)
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )

}



export async function onEvent(event: PluginEvent, { global }: RedshiftMeta) {
    if (!global.pgPool) {
        throw new Error('Plugin is not connected to Redshift cluster!')
    }

    const {
        event: eventName,
        properties,
        $set,
        $set_once,
        distinct_id,
        team_id,
        site_url,
        now,
        sent_at,
        uuid,
        ..._discard
    } = event

    const ip = properties?.['$ip'] || event.ip
    const timestamp = event.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = properties
    let elements = []

    // only move prop to elements for the $autocapture action
    if (eventName === '$autocapture' && properties && '$elements' in properties) {
        const { $elements, ...props } = properties
        ingestedProperties = props
        elements = $elements
    }

    const parsedEvent = {
        uuid,
        eventName,
        properties: JSON.stringify(ingestedProperties || {}),
        elements: JSON.stringify(elements || {}),
        set: JSON.stringify($set || {}),
        set_once: JSON.stringify($set_once || {}),
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp: timestamp
    }

    if (!global.eventsToIgnore.has(eventName)) {
        global.buffer.add(parsedEvent)
    }
}


export const sendBatchToRedshift = async (payload: UploadJobPayload, { global, jobs }: RedshiftMeta) => {
    global.pgPool.query(
        `INSERT INTO ${global.sanitizedTableName} (uuid, event, properties, elements, set, set_once, timestamp,team_id, distinct_id, ip, created_at, site_url)
        VALUES
        ()`
    )
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_]+/g, '')
}