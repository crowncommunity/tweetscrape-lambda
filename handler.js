const AWS = require( 'aws-sdk' );
const awsSignRequests = require( 'aws-sign-requests' );
const fetch = require( 'node-fetch' );

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    SCRAPE_URL,
    INDEX_NAME,
    S3_SAVE_BUCKET,
    SUBMIT_QUEUE,
    SCRAPE_QUEUE,
    ELASTIC_HOST,
    ELASTIC_REGION,
    SCRAPE_LIMIT,
    ACCOUNT_ID
} = process.env;

const tweet_regex = /^\/?(?:\w+\/status\/)?([0-9]+)$/i;
const limit = SCRAPE_LIMIT || 3600;

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

const bucket = new AWS.S3( {
	region: clientconfig.awsConfig.region,
	credentials: clientconfig.awsConfig.credentials,
} )
const queue = new AWS.SQS( {
	region: clientconfig.awsConfig.region,
	credentials: clientconfig.awsConfig.credentials,
} )

module.exports.hello = ( event, context, callback ) => {
	// console.log( 'event', event );
    // console.log( 'context', context);
    console.log('env', process.env)

	const response = {
		statusCode: 200,
		body: JSON.stringify( {
			"message": "World!"
		} ),
	}

	callback( null, response );
}

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
		url: SCRAPE_URL + tweet_path,
	} )

	// console.log(options);
	// console.log(client);

	var search = {
		index: INDEX_NAME,
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
			Bucket: S3_SAVE_BUCKET,
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
			// delete json.tweetData.screenshot

			if ( !up ) return;

            console.log('Tweet Data Successfully uploaded to S3 Bucket');
/**
 *
 * index mapping enabled=false means we can store the image w/o blowing out the index
 *
 * also might need to use explicit insert/update with code scripts on update, for proper data merging
 *
 */
			return client.update( {
				index: INDEX_NAME,
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

module.exports.enqueue = async ( event, context ) => {
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

	var queueUrl = await queue.getQueueUrl( {
			QueueName: SCRAPE_QUEUE,
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

module.exports.submit = async ( event, context ) => {
	// console.log( 'event', event );
    const regex = /(?:(?:https?:\/+)?\w.twitter.com\/)?([\w\d]+\/status\/)?(\d+)/ig;
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

    var queueUrl = await queue.getQueueUrl( {
			QueueName: SUBMIT_QUEUE,
			QueueOwnerAWSAccountId: ACCOUNT_ID
		} )
		.promise()
		.then( data => {
			// console.log('queue url', data)
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

module.exports.submissions = async ( event, context ) => {
    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
    const limit = event.queryStringParameters && event.queryStringParameters[ 'limit' ] || 10;

    const getDedupeMap = messages => {
        var recv = {}
        var del = [];

        messages.map((v, i, a) => {
            if (!v.Body || !v.Body.path) {
                console.log('malformed message', v);
                return;
            }

            // console.log(v.Body.path)

            var matches = v.Body.path.match(/\d+$/);
            if (!matches || !matches[0]) {
                del.push(i);
                return;
            }
            var id = matches[0]

            if (!recv[id] && recv[id] !== 0) {
                recv[id] = i;
                return;
            }

            var c = a[recv[id]];

            if (c.Body.path == v.Body.path) {
                del.push(i);
                return;
            }

            if (id == c.Body.path && id.length < v.Body.path.length) {
                del.push(recv[id]);
                recv[id] = i;
                return
            }

            del.push(i);
        })

        // console.log('nums', messages.length, del, recv);

        return {
            keep: Object.values(recv),
            delete: del
        }
    }

    const delMsgBatch = (queueUrl, batch) => {
        return queue.deleteMessageBatch({
            QueueUrl: queueUrl,
            Entries: batch
        }).promise()
        .catch(err => {
            console.log('delete batch error', err);
            console.log(batch);
        })
        .then(data => {
            console.log('dupes deleted', data);
        })
    }

    const dedupe = async (messages, callback) => {
        var res = getDedupeMap(messages);
        // console.log(res);

        var keepers = res.keep.map(i => {
            return messages[i];
        })
        // console.log('keepers', keepers.length);

        var remove = res.delete.map(i => {
            var msg = messages[i];
            return {
                Id: msg.MessageId,
                ReceiptHandle: msg.ReceiptHandle
            }
        })
        // console.log('deleters', remove.length);

        if (remove && remove.length > 0 ) {
            return callback(remove).then (r => {
                return keepers;
            });
        }

        return Promise.resolve(keepers);
    }

    var queueUrl = await queue.getQueueUrl( {
			QueueName: SUBMIT_QUEUE,
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

    var messages = [];
    var iter = 3;
    for (var i = 20 ; messages.length < limit && iter > 0 && i > 0 ; i--) {
        messages = await queue.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: limit < 10 ? limit : 10
        }).promise()
        .then(resp => {
            if (resp.Messages && resp.Messages.length > 0) {
                resp.Messages = resp.Messages.map(i => {
                    i.Body = i && i.Body ? JSON.parse(i.Body) : null;
                    return i;
                });
            } else {
                resp.Messages = [];
            }
            return resp;
        })
        .then(resp => {
            if (!resp.Messages.length) {
                iter--
            } else {
                messages = messages.concat(resp.Messages)
                iter = 3
            }

            return dedupe(messages, remove => {
                return delMsgBatch(queueUrl, remove)
            });
        })
    }

    // this can result in a messages length larger than the selected limit, but up to 10 messages.
    // if it becomes a problem we can prune the messages list and release the overhang items

    return {
        statusCode: 200,
        body: JSON.stringify(messages)
    }
}

module.exports.deleteSubmssions = async (event, context) => {
    if (event.body) console.log('body', event.body);

    const chunk = (a, s = 10)  => {
        var ret = [];

        for (i = 0 ; i < a.length ; i += s) {
            ret.push(a.slice(i, s));
        }

        return ret;
    }

    const delMsgBatch = (queueUrl, batch) => {
        return queue.deleteMessageBatch({
            QueueUrl: queueUrl,
            Entries: batch
        }).promise()
        .catch(err => {
            console.log('delete batch error', err);
            console.log(batch);
        })
    }

    try {
        if (!event.body) {
            return { statusCode: 400 }
        }
        if(!event.body.match(/^\s*\[(?:\s*\{)?/)) {
            throw new Error('Malformed JSON document');
        }
        event.body = JSON.parse(event.body);

        if (!event.body.length) return { statusCode: 204 }

        var messages = event.body.map(i => {
            if (!i.MessageId || !i.ReceiptHandle) {
                throw new Error ('Malformed Message: field(s) missing');
            }
            if (typeof i.MessageId !== 'string' || typeof i.ReceiptHandle !== 'string') {
                throw new Error ('Malformed Message: malformed field data');
            }
            return {
                Id: i.MessageId,
                ReceiptHandle: i.ReceiptHandle
            }
        });
        event.body = null; // nulling out for memory purposes

        messages = messages.slice(0, 100); // sanity

    } catch (e) {
        console.log('malformed request error', e);
        return { statusCode: 400 }
    }

    try {
        var chunks = chunk(messages);
        messages = null; // nulling out for memory purposes

        // console.log(chunks);

        var queueUrl = await queue.getQueueUrl( {
            QueueName: SUBMIT_QUEUE,
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

        var deletes = await Promise.all(chunks.map(c => {
            return delMsgBatch(queueUrl, c)
        })).then(all => {
            // console.log(all);
            var success = []
            var failed = [];
            all.forEach(i => {
                success = success.concat(i.Successful || []);
                failed = failed.concat(i.Failed || []);
            })

            return {
                Successful: success,
                Failed: failed
            }
        })
    } catch (e) {
        console.log('processing error', e);
        return { statusCode: 500 }
    }

    return {
        statusCode: 200,
        body: JSON.stringify(deletes)
    };
}

const sTpl = (q, fields, boost = 1) => {
    fields = fields.map(i => {
        if (i.match(/^t\./)) {
            return i.replace(/^t\./, 'tweetData.');
        }
        if (i.match(/^q\./)) {
            return i.replace(/^q\./, 'tweetData.quoteTweet.');
        }
        return i;
    })
    if (Array.isArray(q)) {
        q = q.join(' ');
    }
    return {
        query_string: {
            fields: fields,
            query: q,
            boost: boost
        }
    }
}

const quoteQuote = str => {
    const r = m => {
        return "\\" + m;
    }
    const esc = str => {
        return str.replace(/[\(\)\[\]\{\}\!\&\|\:\\]/g, r)
    }
    str = str.replace(/^([^"]*")(?<=^[^"]*")(.*)(?="[^"]*$)("[^"]*$)|^([^"]*")(?<=^[^"]*")(.*)$|^(.*)(?="[^"]*$)("[^"]*$)/,
        (m, p1, p2, p3, p4, p5, p6, p7) => {
            if (p2) {
                return esc(p1) + esc(p2).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p3);
            }
            if (p4) {
                return esc(p4).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p5);
            }
            if (p6) {
                return esc(p6).replace(/(["])/g, "\\\"").replace(/\+\-/g, r) + esc(p7);
            }
            return esc(m).replace(/\+\-/g, r)
        })


    return str;
}

const betterTokenize = str => {
    const s = ' ';
    const q = '"';
    var c = '';
    var mode = s;
    var tokens = []
    str.trim().replace(/\s+/, ' ').split(/(?=[\s"])/i).forEach(i => {
        // if char is not space, append.
        // if char is quote, stay in quote mode until you see another quote

        if (i[0] == s && mode == s) {
            // console.log(c, '->', i);
            tokens.push(quoteQuote(c.trim()));
            c = i;
        } else if (i[0] == q && mode == s) {
            mode = q;
            c += i
        } else if (i[0] == q && mode == q) {
            mode = '';
            c += i
        } else if (i[0] == s && mode == '') {
            // console.log(c, '->', i);
            mode = s;
            tokens.push(quoteQuote(c.trim()));
            c = i
        } else {
            c += i;
        }

        // console.log("(" + i[0] + ")", "(" + mode + ")", i);
    });
    if (c) tokens.push(quoteQuote(c.trim()));

    return tokens;
}

module.exports.search = async (event, context) => {

    const fieldname = 'q';
    var queryString;

    if (event.queryStringParameters) console.log('single parameters', event.queryStringParameters)
	if ( event.queryStringParameters && event.queryStringParameters[ fieldname ] ) {
        queryString = event.queryStringParameters[ fieldname ];
	}

    console.log('queryString', queryString);

    var rawq = betterTokenize(queryString || '');
    var must = rawq.filter(i => i.match(/^\+/))
        .map(i => i.replace(/^\+/, ''));
    var shld = rawq.map(i => i.replace(/^\+/, ''));

    console.log('tokenized', rawq);

    if (!must.length && !shld.length) {
        return { statusCode: 204 }
    }

    const parse2silos = tokens => {
        var tids = tokens.filter(i => i.match(/^"?\d+"?$/))
            .map(i => i.replace(/"/g, ''));
        var name = tokens.filter(i => i.match(/^@"?(\w+)"?$/))
            .map(i => i.replace(/^@"?(\w+)"?$/, (m, p1, o, s) => {
                return p1;
            }));
        var tags = tokens.filter(i => i.match(/^#"?(\w+)"?$/))
            .map(i => i.replace(/^#"?(\w+)"?$/, (m, p1, o, s) => {
                return p1;
            }));
        var mens = tokens.filter(i => i.match(/^(?:m(?:entions?)?|r(?:eplyto)?):"?@?(\w+)"?$/i))
            .map(i => i.replace(/^(?:m(?:entions?)?|r(?:eplyto)?):"?@?(\w+)"?$/i, (m, p1, o, s) => {
                return p1;
            }));
        var repl = tokens.filter(i => i.match(/^r(?:eplyto)?:"?@?(\d+)"?$/i))
            .map(i => i.replace(/^r(?:eplyto)?:"?@?(\d+)"?$/i, (m, p1, o, s) => {
                return p1;
            }));
        tokens = tokens.map(i => i.replace(/^(?:m(?:entions?)?|r(?:eplyto)?):/i, ''))
            .map(i =>  {
                if (!i.match(/"/)) {
                    return i.replace(/[\+\-\(\)\[\]\{\}\!\&\|\:\\]/g,  m => {
                        return "\\" + m;
                    });
                }
                return i;
            });

        return {
            tweetIds: tids,
            userNames: name,
            hashTags: tags,
            mentions: mens,
            inReplyTo: repl,
            normalized: tokens
        }
    }

    const silo2query = silo => {
        silo = parse2silos(silo);

        var q_userNames = silo.userNames && silo.userNames.length ? [
            sTpl(silo.userNames, ["t.screenName"], 2.5),
            sTpl(silo.userNames, ["q.screenName"], 1.75),
            sTpl(silo.userNames, ["t.mentions"], 1.5),
            sTpl(silo.userNames, ["q.mentions"], 1.25),
            sTpl(silo.userNames, ["t.fullName", "q.fullName"], 0.75),
            // sTpl(silo.userNames, ["t.fullName", "q.fullName", "t.tweetText"], 0.75),
            // sTpl(silo.userNames, ["q.tweetText"], 0.5),
        ] : [];

        var q_tweetIds = silo.tweetIds && silo.tweetIds.length ? [
            sTpl(silo.tweetIds, ["t.tweetId"], 2.5),
            sTpl(silo.tweetIds, ["q.tweetId", "t.tweetHTML"], 2.25),
            sTpl(silo.tweetIds, ["t.conversationId", "q.conversationId"], 1.5),
            sTpl(silo.tweetIds, ["q.tweetHTML"], 0.75),
        ] : [];

        // const qd_repl_h = i => {
        //     return '"' + (i.match(/^#/) ? i : ('#' + i)) + '"'
        // }
        var q_hashTags = silo.hashTags && silo.hashTags.length ? [
            sTpl(silo.hashTags, ["tags"], 3),
            sTpl(silo.hashTags, ["t.tweetText"], 2.5),
            sTpl(silo.hashTags, ["q.tweetText"], 2),
            // sTpl(silo.hashTags.map(i => qd_repl_h(i)), ["t.tweetText"], 2.5),
            // sTpl(silo.hashTags.map(i => qd_repl_h(i)), ["q.tweetText"], 2),
        ] : [];

        var q_mentions = silo.mentions && silo.mentions.length ? [
            sTpl(silo.mentions.concat(silo.userNames), ["t.mentions"], 3),
            sTpl(silo.mentions.concat(silo.userNames), ["q.mentions", "q.screenName"], 2.5),
        ] : [];

        var q_inReplyTo = silo.inReplyTo && silo.inReplyTo.length ? [
            sTpl(silo.inReplyTo, ["t.conversationId"], 3),
            // sTpl(silo.inReplyTo, ["t.reply_list", "t.parent_list"], 2.5), // not currently used
            sTpl(silo.inReplyTo, ["q.conversationId", "q.tweetId"], 2.5),
        ] : [];

        var q_normalized = silo.normalized && silo.normalized.length ? [
            sTpl(silo.normalized, ["t.screenName", "t.fullName"], 1.5),
            sTpl(silo.normalized, ["tags", "t.tweetText", "t.mentions"], 1.25),
            sTpl(silo.normalized, ["q.screenName", "q.fullName", "q.tweetText", "q.mentions"], 1),
            sTpl(silo.normalized, ["t.tweetId"], 0.75),
            sTpl(silo.normalized, ["t.conversationId", "q.conversationId"], 0.5),
        ] : [];

        return []
            .concat(q_tweetIds)
            .concat(q_userNames)
            .concat(q_hashTags)
            .concat(q_mentions)
            .concat(q_inReplyTo)
            .concat(q_normalized)
    }

    // console.log(silo2query(shld));

    var query = {
        query : {
            bool: {
                must: must.length ? silo2query(must) : null,
                should: shld.length ? silo2query(shld) : null
            }
        }
    }

    console.log('ES query', query)
    // console.log(JSON.stringify(query));

	var response = await client.search( {
        index: INDEX_NAME,
        body: query
    } );

    // console.log(response);

    return {
        statusCode: 200,
        body: JSON.stringify(response.hits || {})
    }
}

module.exports.initialize = async (event, context) => {

    const mapping = {
        index: INDEX_NAME,
        type: "_doc",
        body: {
            // "_doc": {
                "properties": {
                    "timestamp": {
                        "type": "date"
                    },
                    "tags": {
                        "type": "keyword"
                    },
                    "tweetData": {
                        "properties": {
                            "favorite_num": {
                                "type": "long"
                            },
                            "reply_num": {
                                "type": "long"
                            },
                            "retweet_num": {
                                "type": "long"
                            },
                            "timestamp": {
                                "type": "date"
                            },
                            "tweetHTML": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 1024
                                    }
                                }
                            },
                            "screenshot": {
                                "enabled": false
                            },
                            "quoteTweet": {
                                "properties": {
                                    "quoteHTML": {
                                        "type": "text",
                                        "fields": {
                                            "keyword": {
                                                "type": "keyword",
                                                "ignore_above": 1024
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                // }
            }
        }
    }

    await client.indices.exists( {
       index: INDEX_NAME
    })
        .catch(e => console.log(e))
        .then(r => {
            return !r ? client.indices.create({
                index: INDEX_NAME
            })
            .catch(e => console.log(e))
            .then(res => {
                return client.indices.putMapping( mapping )
            }) : null;
        })
        .then(res => {
            return client.indices.exists( {
                index: INDEX_NAME
            }).catch(e => console.log(e)).then(r => r)
        })
        .then(r => console.log(r));

}
