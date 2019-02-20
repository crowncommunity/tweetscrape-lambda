const AWS = require( 'aws-sdk' );
const isScalar = require('is-scalar');

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    ELASTIC_REGION,
    MODIFICATION_TABLE
} = process.env;


const credentials = DEV_MODE ? new AWS.Credentials(
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
) : new AWS.EnvironmentCredentials( 'AWS' )

const db = new AWS.DynamoDB( {
    region: ELASTIC_REGION,
    credentials: credentials,
    correctClockSkew: true,
})

const normalize = obj => {
    let normalized = {}
    // let types = {}

    Object.keys(obj).forEach(field => {
        Object.keys(obj[field]).forEach(dumb => {
            switch (dumb) {
                case "S":
                case "N":
                case "B":
                case "BOOL":
                case "SS":
                case "SN":
                case "SB":
                    normalized[field] = obj[field][dumb]
                    return; break;
                case "L":
                    let again = obj[field][dumb]
                    normalized[field] = again.map(item => {
                        var l
                        Object.keys(item).forEach(dumb => { // there should only be one item here anyway. we don't care about data type
                            l = item[dumb]
                        })
                        return l;
                    });
                    return; break;
                case "M":
                    throw new Error ('Map data type not supported');
                case "N":
                    normalized[field] = null;
                    return; break;
                default:
                    throw new Error ('unknown field format ' + dumb);
            }

            // types[field] = dumb == 'S' ? 'string' : (
            //     dumb == 'N' ? 'numeric' : (
            //     dumb == 'B' ? 'binary' : dumb
            // ))
        })
    })

    return {
        normalized: normalized,
        // field_types: types
    };
}

const extractTweetId = event => {
    if (event.pathParameters) console.log('path parameters', event.pathParameters)
    if (!event.pathParameters || !event.pathParameters['tweet_id']) {
        throw new Error('Missing PATH parameter')
    }

    return event.pathParameters['tweet_id'];
}

const extractBody = event => {
    if (event.body) console.log('Event Body', event.body)
    if ( event.body && event.headers && (
        // not sure which is the problem, but either AWS or Postman is down casing the header... lame
        (event.headers[ 'Content-Type' ] && event.headers[ 'Content-Type' ] == 'application/json') ||
        (event.headers[ 'content-type' ] && event.headers[ 'content-type' ] == 'application/json')
    ) ) {
        const body = JSON.parse( event.body || '' )

        if (typeof body != 'object') {
            throw new Error ('request body must be a JSON object')
        }

        validateBody(body);

        console.log('parsed body', body)

        return body
    }

    throw new Error('Invalid request body.')
}

const validateBody = body => {
    const keys = Object.keys(body);

    if (!keys.length) {
        throw new Error ('Missing Body data');
    }

    keys.forEach(field => {
        // console.log(field, body[field]);
        if (isScalar(body[field])) return;
        if (Array.isArray(body[field])) {
            body[field].forEach(row => {
                if (!isScalar(body[field][row])) {
                    throw new Error('Invalid body field array. All fields must be scalar or an array of scalars')
                }
            })
            return
        }

        throw new Error('Invalid body field. All fields must be scalar or an array of scalars')
    })
}

const getMostRecent = async tweet_id => {
    return db.query({
        TableName: MODIFICATION_TABLE,
        ScanIndexForward: false,
        Limit: 1,
        KeyConditions: {
            TweetId: {
                ComparisonOperator: 'EQ',
                AttributeValueList: [ { N: tweet_id }]
            }
        }
    })
    .promise()
    .catch(err => console.log)
    .then(res => {
        return res.Count && normalize(res.Items[0]).normalized
    })
}

const formatRecord = (record, event) => {
    event.timestamp++
    return {
        TweetId:    { [schema.TweetId]   : record.TweetId.toString() },
        Timestamp:  { [schema.Timestamp] : event.timestamp.toString() },
        User:       { [schema.User]      : JSON.stringify(event.requestContext.identity || '') },
        Parent:     { [schema.Parent]    : (record.Parent || 0).toString() },
        Action:     { [schema.Action]    : record.Action },
        Field:      { [schema.Field]     : record.Field },
        Content:    { [schema.Content]   : isScalar(record.Content) ? record.Content : JSON.stringify(record.Content) },
    }
}

const schema = {
    TweetId: 'N',
    Timestamp: 'N',
    User: 'S',
    Parent: 'N',
    Action: 'S',
    Field: 'S',
    Content: 'S'
}

module.exports.get = async ( event, context ) => {
}

module.exports.post = async ( event, context ) => {
    // console.log('event', event);
    try {
        var action;
        switch (event.httpMethod) {
            case 'POST':
                action = 'INSERT';
                break;
            case 'PUT':
                action = 'REPLACE';
                break;
            case 'PATCH':
                action = 'APPEND';
                break;
            case 'DELETE':
                action = 'REMOVE';
                break;
            default:
                throw new Error('Invalid HTTP Method')
        }

        var tweet_id  = extractTweetId(event)
        var body      = extractBody(event);
        var fields    = Object.keys(body);
    } catch (e) {
        console.log(e);
        return {statusCode: 400}
    }

    event.timestamp = Math.floor(Date.now() / 1000) * 1000;

    // console.log('here', tweet_id, fields, body);

    const most_recent = await getMostRecent(tweet_id)

    // console.log('most recent', most_recent);

    if (action == 'INSERT' && most_recent) {
        return {statusCode: 409}
    } else if (action == 'REMOVE' && !most_recent) {
        return {statusCode: 404}
    }

    var errors = []

    console.log('fields', fields);

    var ts;
    var all = fields.map(field => {
        var ret = {
            PutRequest: {
                Item: formatRecord({
                    TweetId: tweet_id,
                    Action: (action == 'REMOVE' && !body[field]) ? 'DELETE' : action,
                    Field: field,
                    Parent: ts || most_recent.Timestamp || null,
                    Content: body[field]
                }, event)
            }
        }

        ts = ret.PutRequest.Item.Timestamp.N

        if (action == 'REMOVE' && !body[field]) {
            delete ret.PutRequest.Item.Content
        }

        return ret;
    });

    if(!all) {
        return {statusCode: 400}
    }

    // console.log(all);

    var response = await db.batchWriteItem({
        ReturnConsumedCapacity: 'TOTAL',
        RequestItems : {
            [MODIFICATION_TABLE]: all
        }
    }).promise()
    .catch(err => {
        errors.push(err);
    })
    .then(resp => {
        return resp ? {
            UnprocessedItems: resp.UnprocessedItems,
            Processed: resp.ConsumedCapacity.length > 0
        } : null
    })

    if (errors.length) console.log('errors', errors);

    if (response && errors.length) response.errors = errors;

    return response ? {
        statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(response || []),
    } : {
        statusCode: 500,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(errors ? {
            errors: errors
        } : []),
    }
}
