const AWS = require( 'aws-sdk' );
const awsSignRequests = require( 'aws-sign-requests' );
const fetch = require( 'node-fetch' );

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const QUEUE_NAME = 'ScrapeQueue'
const ACCOUNT_ID = '929366106277'

const tweet_root = process.env.SCRAPE_URL;
const tweet_regex = /^\/?(?:\w+\/status\/)?([0-9]+)$/i;
const limit = DEV_MODE ? ( process.env.SCRAPE_LIMIT_LOCAL || 30 ) : ( process.env.SCRAPE_LIMIT || 3600 );

const fieldname = 'tweet';

const clientconfig = {
	hosts: DEV_MODE ? process.env.ELASTIC_HOST_LOCAL : process.env.ELASTIC_HOST,
	connectionClass: AWS_class,
	awsConfig: new AWS.Config( {
		region: process.env.ELASTIC_REGION,
		credentials: DEV_MODE ? new AWS.Credentials(
			process.env.AWS_ACCESS_KEY_ID,
			process.env.AWS_SECRET_ACCESS_KEY
		) : new AWS.EnvironmentCredentials( 'AWS' ),
	} )
}
const client = ES.Client( clientconfig )

const bucket = new AWS.S3( {
	region: clientconfig.awsConfig.region,
	credentials: clientconfig.awsConfig.credentials,
} )
const queue = new AWS.SQS( {
	region: clientconfig.awsConfig.region,
	credentials: clientconfig.awsConfig.credentials,
} )

// console.log(process.env);
// console.log(queue);
// console.log(clientconfig);


const doScrape = async ( event, context ) => {
	var tweet_path;
	var tweet_id;

	console.log('Scrape Event', event);

	try {
		switch ( typeof event ) {
		case "object":
			if ( !event.path || !tweet_regex.test( event.path ) ) {
				throw new Error( 'Invalid Event. Object must include valid "path" varaible.' );
			}
			tweet_path = event.path.replace( /^\/+|\/+$/g, '' );
			break;

		case "string":
			if ( !tweet_regex.test( event ) ) {
				throw new Error( 'Invalid Event. String must be a valid tweet path.' );
			}
			event = event.replace( /^\/+|\/+$/g, '' );
		case "number":
			tweet_path = event;
			break;

		default:
			console.log( typeof event );
			throw new Error( 'Invalid Event' );
		}

		if ( !tweet_path ) {
			throw new Error( 'Missing Tweet Path' );
		}

		switch ( typeof tweet_path ) {
		case 'string':
			var found = tweet_path.match( tweet_regex );
			if ( !found[ 1 ] ) {
				throw new Error( 'Malformed Tweet String' );
			}
			tweet_id = found[ 1 ];
			break;

		case 'number':
			tweet_id = tweet_path;
			break;

		default:
			throw new Error( 'Malformed Tweet Path.' );
		}

	}
	catch ( err ) {
		console.log( 'Tweet Parse Error', err );
		// context.fail();
		return {
			result: -1,
			message: e.message,
			event
		}
	}

    console.log('Tweet to scrape:', tweet_path );

	let credentials = clientconfig.awsConfig.credentials;

	// console.log(credentials);

	const options = awsSignRequests( {
		credentials: {
			access_key: credentials.accessKeyId,
			secret_key: credentials.secretAccessKey,
			session_token: credentials.sessionToken,
		},
		url: tweet_root + tweet_path,
	} )

	// console.log(options);
	// console.log(client);

	var search = {
		index: process.env.INDEX_NAME,
		type: '_doc',
		id: tweet_id,
		_source: 'timestamp',
	};
	var existing = await client.exists( search )
		.then( res => {
			return res ? client.get( search ) : false;
		} )
		.catch( err => {
			console.log( 'Tweet Data Check Error', err );
            throw err;
		} )
	if ( existing && existing._source && existing._source.timestamp ) {
		var now = Math.floor( ( Date.now() / 1000 | 0 ) / limit );
		var then = Math.floor( existing._source.timestamp / limit );
		// console.log(now, then);

		if ( now == then ) {
			// context.done();
            console.log('Throttle Limit Hit for Tweet', tweet_id)
			return {
				result: 0,
				message: 'Throttle Limit',
				event
			}
		}
	}

	var json = await fetch( options.url, options )
		.then( res => res.json() )
		.then( json => {
            console.log('Tweet Successfully Scraped');
			json.timestamp = Date.now() / 1000 | 0;

			// remove nulls, so they don't overwrite previous versions of the document
			for ( var key of Object.keys( json.tweetData ) ) {
				if ( json.tweetData[ key ] === null ) {
					// console.log(key, json.tweetData[key]);
					delete json.tweetData[ key ];
				}
			}

			return json.tweetData ? json : null
		} )
		.catch( err => {
			console.log('Tweet Scrape Error', err );
			return err.message;
		} )

	// this is going to need something to store/handle bad tweet IDs

	if ( !json || typeof json == 'string' || !json.tweetData ) {
		// context.fail();
        console.log("Tweet Scrape Content Failure", tweet_id);
		return {
			result: -1,
			message: json,
			event
		}
	}

	var resp = await bucket.putObject( {
			Bucket: process.env.S3_SAVE_BUCKET,
			Key: json.tweetData.tweetId + '.json',
			Body: JSON.stringify( json ),
			ContentType: 'application/json',
			ACL: 'public-read',
		} )
		.promise()
		.catch( e => {
			console.log( 'S3 Upload Error', e );
		} )
		.then( up => {
			delete json.tweetData.screenshot

			if ( !up ) return;

            console.log('Tweet Data Successfully uploaded to S3 Bucket');

			return client.update( {
				index: process.env.INDEX_NAME,
				type: '_doc',
				id: json.tweetData.tweetId,
				body: {
					doc: json,
					doc_as_upsert: true
				}
			} )
		} )
		.then( resp => {
            console.log('Success', resp)
			return resp;
		} )

	// context.succeed();
	return {
		result: 1,
		message: 'successful test',
		event,
		result: resp
	};
}

module.exports.scrape = async ( event, context ) => {
	// console.log('successful invokation');
	console.log( 'event', typeof event, event );

	// await client.indices.delete({
	//     index: '*'
	// }); return;

	var result;

	if ( event.Records ) {
		result = [];
		for ( var i in event.Records ) {
			try {
				var record = event.Records[ i ];

				record.body = /^\d+$/.test( record.body ) ? record.body : JSON.parse( record.body );

                console.log('Processing Tweet Data', record.body);
				var res = await doScrape( record.body, context );
				// console.log('res', res);

				result.push( res );
			}
			catch ( err ) {
				console.log('Record Parse Error', err)
			}
		}
        console.log('Number of Scrape Results', result.length);
        console.log('Scrape Results', result);

		if ( !result.length ) {
			context.done();
			return;
		}

		const hasFail = result.filter( res => res.result == -1 );
		if ( hasFail.length > 0 ) {
			context.fail();
			return result;
		}

		const hasDone = result.filter( res => res.result == 0 );
		if ( hasDone.length > 0 ) {
			context.done();
			return result;
		}

		context.succeed();
		return result;

	}
	else {
        console.log('Processing Submitted Tweet', event);
		result = await doScrape( event, context );

        console.log('Scrape Result', result);
		switch ( result.result ) {
		case 1:
			context.succeed();
			break;
		case 0:
			context.done();
			break;
		default:
			context.done(); // temporary until I make the API give a meaningful error back
			// context.fail();
		}
		return result;
	}
};

module.exports.hello = ( event, context, callback ) => {
	console.log( 'event', event );

	const response = {
		statusCode: 200,
		headers: {
			"x-custom-header": "yo mama"
		},
		body: JSON.stringify( {
			"message": "You Suck"
		} ),
	}

	callback( null, response );
}

module.exports.enqueue = async ( event, context ) => {
	// console.log('event', event);

	const regex = /\/?([\w\d]+\/status\/)?(\d+)/i;

	const validate = tweet => {
		if ( typeof tweet == 'number' ) {
			return {
				[ String( tweet ) ]: null
			}
		}
		if ( tweet ) {
			var matches = tweet.match( regex )
			return matches ? {
				[ matches[ 2 ] ]: matches[ 1 ] || null
			} : null;
		}
	}

	var tweets = [];

    if (event.body) console.log('Event Body', event.body)
	if ( event.headers && event.headers[ 'Content-Type' ] && event.headers[ 'Content-Type' ] == 'application/json' ) {
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

	var queueUrl = await queue.getQueueUrl( {
			QueueName: QUEUE_NAME,
			QueueOwnerAWSAccountId: ACCOUNT_ID
		} )
		.promise()
		.then( data => {
			console.log('queue url', data)
			return data.QueueUrl;
		} )
		.catch( err => {
			console.log( 'url error', err );
			context.fail();
		} )

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

// ADD AN ENQUEUE FUNCTION AND CONNECT TO AWS API THINGY
