import { createBuffer } from '@posthog/plugin-contrib'
import { Plugin, PluginMeta, ProcessedPluginEvent, RetryError } from '@posthog/plugin-scaffold'
import { Client } from 'pg'

type RedshiftPlugin = Plugin<{
    global: {
        pgClient: Client
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
        uploadSeconds: string
        uploadMegabytes: string
        eventsToIgnore: string
        propertiesDataType: string
    }
}>

type RedshiftMeta = PluginMeta<RedshiftPlugin>

interface ParsedEvent {
    uuid: string
    eventName: string
    properties: string
    elements: string
    set: string
    set_once: string
    distinct_id: string
    team_id: number
    ip: string
    site_url: string
    timestamp: string
}

type InsertQueryValue = string | number

export const setupPlugin: RedshiftPlugin['setupPlugin'] = async (meta) => {
    const { global, config } = meta

    const requiredConfigOptions = ['clusterHost', 'clusterPort', 'dbName', 'dbUsername', 'dbPassword']
    for (const option of requiredConfigOptions) {
        if (!(option in config)) {
            throw new Error(`Required config option ${option} is missing!`)
        }
    }

    if (!config.clusterHost.endsWith('redshift.amazonaws.com')) {
        throw new Error('Cluster host must be a valid AWS Redshift host')
    }

    // Max Redshift insert is 16 MB: https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html
    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 10))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 600))

    global.sanitizedTableName = sanitizeSqlIdentifier(config.tableName)

    const propertiesDataType = config.propertiesDataType === 'varchar' ? 'varchar(65535)' : 'super'

    const queryError = await executeQuery(
        `CREATE TABLE IF NOT EXISTS public.${global.sanitizedTableName} (
            uuid varchar(200),
            event varchar(200),
            properties ${propertiesDataType},
            elements varchar(65535),
            set ${propertiesDataType},
            set_once ${propertiesDataType},
            timestamp timestamp with time zone,
            team_id int,
            distinct_id varchar(200),
            ip varchar(200),
            site_url varchar(200)
        );`,
        [],
        config
    )

    if (queryError) {
        throw new Error(`Unable to connect to Redshift cluster and create table with error: ${queryError.message}`)
    }

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
}

export async function exportEvents(events: ProcessedPluginEvent[], meta: RedshiftMeta) {
    const eventsToExport = events.filter((event) => !meta.global.eventsToIgnore.has(event.event))
    const parsedEvents = eventsToExport.map(parseEvent)

    await insertBatchIntoRedshift(parsedEvents, meta)
}

export const parseEvent = (event: ProcessedPluginEvent): ParsedEvent => {
    const { event: eventName, properties, $set, $set_once, distinct_id, team_id, uuid, timestamp, ..._discard } = event

    const ip = properties?.['$ip'] || event.ip
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
        site_url: '',
        timestamp: new Date(timestamp).toISOString(),
    }

    return parsedEvent
}

export const insertBatchIntoRedshift = async (events: ParsedEvent[], { global, config }: RedshiftMeta) => {
    let values: InsertQueryValue[] = []
    let valuesString = ''

    function getPropsInsertString(stringifiedValue: string) {
        return config.propertiesDataType === 'super' ? `JSON_PARSE(${stringifiedValue})` : stringifiedValue
    }

    for (let i = 0; i < events.length; ++i) {
        const { uuid, eventName, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp } =
            events[i]

        // Creates format: ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11), ($12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        valuesString += ' ('
        for (let j = 1; j <= 11; ++j) {
            valuesString += `$${11 * i + j}${j === 11 ? '' : ', '}`
        }
        valuesString += `)${i === events.length - 1 ? '' : ','}`

        values = [
            ...values,
            ...[
                uuid,
                eventName,
                getPropsInsertString(properties),
                elements,
                getPropsInsertString(set),
                getPropsInsertString(set_once),
                distinct_id,
                team_id,
                ip,
                site_url,
                timestamp,
            ],
        ]
    }

    console.log(`Flushing ${events.length} event${events.length > 1 ? 's' : ''} to RedShift`)

    const queryError = await executeQuery(
        `INSERT INTO ${global.sanitizedTableName} (uuid, event, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp)
        VALUES ${valuesString}`,
        values,
        config
    )

    if (queryError) {
        console.error(`Error uploading to Redshift: ${queryError.message}. Setting up retries...`)
        throw new RetryError()
    }
}

const executeQuery = async (query: string, values: any[], config: RedshiftMeta['config']): Promise<Error | null> => {
    const pgClient = new Client({
        user: config.dbUsername,
        password: config.dbPassword,
        host: config.clusterHost,
        database: config.dbName,
        port: parseInt(config.clusterPort),
    })

    let error: Error | null = null
    try {
        await pgClient.connect()
        await pgClient.query(query, values)
    } catch (err) {
        error = err as Error
    } finally {
        await pgClient.end()
    }

    return error
}

export const teardownPlugin: RedshiftPlugin['teardownPlugin'] = ({ global }) => {
    global.buffer.flush()
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_.]+/g, '')
}
