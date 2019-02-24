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

module.exports.main = async ( event, context ) => {

	const mapping = {
		index: INDEX_NAME,
		type: "_doc",
		body: {
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
			}
		}
	}

    console.log('initializing index', INDEX_NAME)

    var exists = await client.indices.exists( {
        index: INDEX_NAME
    } )
    .catch( e => {
        console.log('index check error', e )
        context.done();
        return false;
    } )

    console.log('exists?', exists)
    if (exists) {
        context.done();
        return true;
    }

	exists = await client.indices.create( {
        index: INDEX_NAME
    } )
    .catch( e => {
        console.log('index create failure', e )
        context.done();
        return false;
    } )

    console.log('exists?', exists)
    if (!exists) {
        context.done();
        return false;
    }

    exists = client.indices.putMapping( mapping )

    console.log('mapping', exists);

    context.done();
    return exists
}
