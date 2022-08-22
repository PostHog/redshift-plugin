import { insertBatchIntoRedshift, exportEvents, parseEvent } from './index'

const mockedPgClient = {
    connect: jest.fn(),
    query: jest.fn(),
    end: jest.fn(),
}
jest.mock('pg', () => ({
    Client: jest.fn(() => mockedPgClient),
}))

describe('Redshift Export Plugin', () => {
    let mockedMeta: any

    beforeEach(() => {
        mockedMeta = {
            global: {
                buffer: {
                    add: jest.fn(),
                },
                eventsToIgnore: new Set(['ignore me']),
                sanitizedTableName: 'my_table',
            },
            config: {
                clusterHost: 'some.host',
                clusterPort: '9410',
                dbName: 'mydb',
                tableName: 'mytable',
                dbUsername: 'admin',
                dbPassword: 'strongpass123',
                propertiesDataType: 'varchar',
            },
        }
        jest.clearAllMocks()
    })

    describe('insertBatchIntoRedshift()', () => {
        test('inserts data into redshift', async () => {
            const events = [
                {
                    eventName: 'test',
                    properties: '{}',
                    distinct_id: 'did1',
                    team_id: 1,
                    uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                    ip: '127.0.0.1',
                    timestamp: '2022-08-18T15:42:32.597Z',
                    set: '{}',
                    set_once: '{}',
                    elements: '',
                    site_url: '',
                },
                {
                    eventName: 'test2',
                    properties: '{}',
                    distinct_id: 'did1',
                    team_id: 1,
                    uuid: '37114ebb-7b13-4301-b859-0d0bd4d5c7e5',
                    ip: '127.0.0.1',
                    timestamp: '2022-08-18T15:42:32.597Z',
                    set: '{}',
                    set_once: '{}',
                    elements: '',
                    site_url: '',
                },
            ]
            await insertBatchIntoRedshift(events, mockedMeta)

            expect(mockedPgClient.query).toHaveBeenCalledWith(
                `INSERT INTO my_table (uuid, event, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp)
        VALUES  ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11), ($12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
                [
                    '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                    'test',
                    '{}',
                    '',
                    '{}',
                    '{}',
                    'did1',
                    1,
                    '127.0.0.1',
                    '',
                    '2022-08-18T15:42:32.597Z',
                    '37114ebb-7b13-4301-b859-0d0bd4d5c7e5',
                    'test2',
                    '{}',
                    '',
                    '{}',
                    '{}',
                    'did1',
                    1,
                    '127.0.0.1',
                    '',
                    '2022-08-18T15:42:32.597Z',
                ]
            )
        })
    })

    describe('parseEvent()', () => {
        test('converts ProcessedPluginEvent to ParsedEvent', () => {
            const event = {
                event: 'test',
                properties: { foo: 'bar' },
                distinct_id: 'did1',
                team_id: 1,
                uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
                ip: '127.0.0.1',
                timestamp: '2022-08-18T15:42:32.597Z',
            }

            const parsedEvent = parseEvent(event)

            expect(parsedEvent).toEqual({
                distinct_id: 'did1',
                elements: '[]',
                eventName: 'test',
                ip: '127.0.0.1',
                properties: '{"foo":"bar"}',
                set: '{}',
                set_once: '{}',
                site_url: '',
                team_id: 1,
                timestamp: '2022-08-18T15:42:32.597Z',
                uuid: '37114ebb-7b13-4301-b849-0d0bd4d5c7e5',
            })
        })
    })
})
