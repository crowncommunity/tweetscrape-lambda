const AWS = require( 'aws-sdk' );
const fetch = require( 'node-fetch' );
const path = require('path');
const isScalar = require('is-scalar');

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    ELASTIC_REGION,
    WEBARCHIVE_QUEUE,
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

const regex = /(?:(?:https?:\/+)?(?:\w\.)?twitter.com\/|\/)?([\w\d]+\/status\/)?(\d+)/ig;

const formatRecord = (record) => {
    ts = Math.floor(Date.now() / 1000) * 1000;
    return {
        TweetId:    { [schema.TweetId]   : record.TweetId.toString() },
        Timestamp:  { [schema.Timestamp] : ts.toString() },
        User:       { [schema.User]      : 'system' },
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
        return res.Count && res.Items.length && normalize(res.Items[0]).normalized || null
    })
}

const doArchive = async (event, context) => {
    console.log('do archive', event);
    if (!event) {
        console.log('missing event')
    }
    if (!event.tweet) {
        console.log('missing tweet')
    }
    if (!regex.test(event.tweet)) {
        console.log('invalid tweet');
    }

    const url = 'https://web.archive.org/save/http://twitter.com' + event.tweet;
    const tweet_id = path.basename(event.tweet)

    var archive_url = await fetch(url, {
        method: 'get',
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36 Chrome/71.0.0.0"
        }
    })
    .then(res => {
        return res.headers.get('content-location')
    })
    .catch(err => console.log)

    if (!archive_url) {
        throw new Error('missing redirect url from webarchive');
    }

    const most_recent = await getMostRecent(tweet_id)

    console.log('most_recent', most_recent);

    var record = {
        TweetId: tweet_id,
        Action: 'REPLACE',
        Field: 'archive_url',
        Parent: most_recent && most_recent.Timestamp || null,
        Content: 'http://web.archive.org' + archive_url
    }
    // console.log(record);

    return await db.putItem({
        ReturnConsumedCapacity: 'TOTAL',
        TableName: MODIFICATION_TABLE,
        Item : formatRecord(record)
    }).promise()
    .catch(err => {
        console.log(err)
    })
    .then(resp => {
        console.log(resp)
    })
}

module.exports.main = async ( event, context ) => {
	// console.log('successful invokation');
	console.log( 'event', typeof event, event );

	var result;

	if ( event.Records ) {
		result = [];
		for ( var i in event.Records ) {
			try {
				var record = event.Records[ i ];

                record.body = JSON.parse( record.body )
				if (!record.body) {
                    throw new Error('body doesnt parse')
                }

                console.log('Processing Url Data', record.body);
				result = await doArchive( record.body, context );
                console.log('Result', result);
			}
			catch ( err ) {
				console.log('Record Parse Error', err)
			}
		}

        context.done();
	}
	else {
        console.log('Processing Submitted Url', event);
		result = await doArchive( event, context );

        console.log('Result', result);
        context.done();
	}
};
