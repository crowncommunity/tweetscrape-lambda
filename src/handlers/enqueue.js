const AWS = require( 'aws-sdk' );

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    SCRAPE_QUEUE,
    ACCOUNT_ID,
    ELASTIC_REGION,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
} = process.env;

const config = new AWS.Config( {
	region: ELASTIC_REGION,
	credentials: DEV_MODE ? new AWS.Credentials(
		AWS_ACCESS_KEY_ID,
		AWS_SECRET_ACCESS_KEY
	) : new AWS.EnvironmentCredentials( 'AWS' ),
} )

const queue = new AWS.SQS( {
    region: config.region,
    credentials: config.credentials,
} )

const getQueueUrl = async context => {
    return queue.getQueueUrl( {
        QueueName: SCRAPE_QUEUE,
		QueueOwnerAWSAccountId: ACCOUNT_ID
	})
	.promise()
	.then( data => {
		console.log('queue url', data)
		return data.QueueUrl;
	} )
	.catch( err => {
		console.log( 'url error', err );
		context.fail();
	} )
}

module.exports.main = async ( event, context ) => {
	// console.log('event', event);

	const regex = /\/?([\w\d]+\/status\/)?(\d+)/i;
    const fieldname = 'tweet';

	const validate = tweet => {
        // console.log('validate', tweet);
		if ( typeof tweet == 'number' ) {
            // console.log('type of', typeof tweet);
			return {
				[ String( tweet ) ]: null
			}
		}
		if ( tweet ) {
			var matches = tweet.match( regex )
            // console.log('matches', matches);
			return matches ? {
				[ matches[ 2 ] ]: matches[ 1 ] || null
			} : null;
		}
	}

	var tweets = [];

    if (event.body) console.log('Event Body', event.body)
	if ( event.headers && (
        // not sure which is the problem, but either AWS or Postman is down casing the header... lame
        (event.headers[ 'Content-Type' ] && event.headers[ 'Content-Type' ] == 'application/json') ||
        (event.headers[ 'content-type' ] && event.headers[ 'content-type' ] == 'application/json')
    ) ) {
        try {
            event.body = JSON.parse( event.body );
            console.log('parsed body', event.body)

            if ( event.body && event.body[ fieldname ] && typeof event.body[ fieldname ].length ) {
                tweets = tweets.concat( event.body.tweet.map( i => validate( i ) ) );
            }
        } catch (e) {
            console.log('Event Body Parse Error', e)
        }
	}

    if (event.multiValueQueryStringParameters) console.log('multi params', event.multiValueQueryStringParameters)
	if ( event.multiValueQueryStringParameters && event.multiValueQueryStringParameters[ fieldname ] && typeof event.multiValueQueryStringParameters[ fieldname ].length ) {
		tweets = tweets.concat( event.multiValueQueryStringParameters[ fieldname ].map( i => validate( i ) ) );
	}

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
	if ( event.queryStringParameters && event.queryStringParameters[ fieldname ] ) {
		tweets.push( validate( event.queryStringParameters[ fieldname ] ) );
	}

	var u_tweet = {};
	tweets.forEach( tweet => {
		if ( tweet ) {
			var key = Object.keys( tweet )[ 0 ];
			if ( !u_tweet[ key ] ) {
				u_tweet[ key ] = ( tweet[ key ] || '' ) + key;
			}
			else if ( tweet[ key ] && u_tweet[ key ].length == key ) {
				u_tweet[ key ] = tweet[ key ] + key;
			}
		}
	} )
	tweets = Object.values( u_tweet );

    console.log('tweets to scrape', tweets)

	var queueUrl = await getQueueUrl(context)

	var errors = []

	// this should be turned into a SendMessageBatch call but fuck it for now
	var tweets = tweets.map( tweet => {
		return queue.sendMessage( {
				MessageBody: JSON.stringify( {
					path: tweet
				} ),
				QueueUrl: queueUrl,
			} )
			.promise()
			.catch( err => {
				console.log( 'message error', err );
				errors.push( {
					tweet: tweet,
					error: err
				} );
				return false;
			} )
			.then( data => {
                console.log('message sent', data)
				return data ? true : false;
			} )
	} );

	return Promise.all( tweets )
		.then( resp => {
			const good = resp.filter( i => i )
				.length;
			const all = resp.length;

			return {
				statusCode: 200,
                headers: {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Credentials': true,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify( {
					success: good,
					fail: all - good,
					errors: errors
				} )
			}
		} )
		.catch( err => {
			errors.push( 'promise error', err )

			console.log( errors );
			// context.fail();
		} )
}
