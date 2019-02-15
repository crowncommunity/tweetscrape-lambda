const AWS = require( 'aws-sdk' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    SUBMIT_QUEUE,
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
        QueueName: SUBMIT_QUEUE,
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
	// console.log( 'event', event );
    const regex = /(?:(?:https?:\/+)?(?:\w\.)?twitter.com\/)?([\w\d]+\/status\/)?(\d+)/ig;
    const fieldname = 'tweet';
    const ts = Date.now() / 1000

    var raw_tweets = [];

    if (event.multiValueQueryStringParameters) console.log('multi params', event.multiValueQueryStringParameters)
	if ( event.multiValueQueryStringParameters && event.multiValueQueryStringParameters[ fieldname ] && typeof event.multiValueQueryStringParameters[ fieldname ].length ) {
		raw_tweets = event.multiValueQueryStringParameters[ fieldname ] || []
	}

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
	if ( event.queryStringParameters && event.queryStringParameters[ fieldname ] ) {
		raw_tweets.push(event.queryStringParameters[ fieldname ])
	}

    if (event.pathParameters) console.log('path parameters', event.pathParameters)
    if (event.pathParameters && event.pathParameters[fieldname]) {
        raw_tweets.push(decodeURIComponent(event.pathParameters[fieldname]))
    }

    console.log('# attempted tweets', raw_tweets.length);
    // sanity limit
    raw_tweets = raw_tweets.slice(0, 1000);

    var tweets = [];
    raw_tweets.forEach(tweet => {
        var matches = tweet.match(regex);
        tweets = tweets.concat(matches);
    })

    // console.log(tweets);

    var u_tweet = {};
	tweets.forEach( tweet => {
		if ( tweet ) {
            var matches = typeof tweet == 'numeric' ? [tweet, null, tweet] : tweet.match(/(?:([\w\d]+)\/status\/)?(\d+)/)

            if (matches[2] && !u_tweet[matches[2]]) {
                u_tweet[matches[2]] = matches[1] || null
            }
		}
	} )
	tweets = [];
    for ( var key of Object.keys( u_tweet ) ) {
        tweets.push(!u_tweet [key] ? key : u_tweet[key] + '/status/' + key)
    }

    tweets = tweets.slice(0, 100);
    console.log('# tweets to submit', tweets.length)
    // sanity Limit

    var queueUrl = await getQueueUrl(context)

	var errors = []

	// this should be turned into a SendMessageBatch call but fuck it for now
	var tweets = tweets.map( tweet => {
		return queue.sendMessage( {
            // we include timestamp and context to help mitigate bad behavior
				MessageBody: JSON.stringify( {
					path: tweet,
                    ts: ts,
                    context: event.requestContext || null
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
                console.log('message sent', tweet, data)
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
                    'Content-Type': 'application/json',
                    'X-Test-Header': 'wat',
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
