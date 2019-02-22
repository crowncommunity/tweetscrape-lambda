const AWS = require( 'aws-sdk' );

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    INDEX_NAME,
    ELASTIC_HOST,
    ELASTIC_REGION
} = process.env;

// console.log('env', {
//     AWS_ACCESS_KEY_ID,
//     AWS_SECRET_ACCESS_KEY,
//     INDEX_NAME,
//     ELASTIC_HOST,
//     ELASTIC_REGION
// })

const clientconfig = {
    hosts: ELASTIC_HOST,
	connectionClass: AWS_class,
	awsConfig: new AWS.Config( {
		region: ELASTIC_REGION,
		credentials: DEV_MODE ? new AWS.Credentials(
			AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY
		) : new AWS.EnvironmentCredentials( 'AWS' ),
	} )
}

const client = ES.Client( clientconfig )

const extractTweetId = event => {
    if (event.pathParameters) console.log('path parameters', event.pathParameters)
    if (!event.pathParameters || !event.pathParameters['tweet_id']) {
        throw new Error('Missing PATH parameter')
    }

    return event.pathParameters['tweet_id'];
}


module.exports.main = async (event, context) => {
    try {
        var tweet_id = extractTweetId(event);
        if (tweet_id.match(/[^\d]/)) {
            throw new Error('invalid tweet id');
        }
    } catch (e) {
        return {
            statusCode: 400,
            body: JSON.stringify(e.message || 'malformed request')
        }
    }

    try {
        const search = {
            index: INDEX_NAME,
            type: '_doc',
            id: tweet_id
        }

        console.log('search', search);

        var doc = await client.get(search)
        .catch(err => {
            // console.log(err);
            if (!err.status || err.status !== 404) {
                throw err;
            }
        })
        .then (doc => {
            if (doc && doc._source) {
                delete doc._source.metadata
                delete doc._source.parents
                delete doc._source.replies

                return doc._source
            }
        })
    } catch (e) {
        console.log(e)
        return {
            statusCode: e.status,
            body: JSON.stringify(e.message || 'error')
        }
    }

    return {
        statusCode: doc ? 200 : 404,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(doc || 'not found')
    }
}
