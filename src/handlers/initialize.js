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

	await client.indices.exists( {
			index: INDEX_NAME
		} )
		.catch( e => console.log( e ) )
		.then( r => {
			return !r ? client.indices.create( {
					index: INDEX_NAME
				} )
				.catch( e => console.log( e ) )
				.then( res => {
					return client.indices.putMapping( mapping )
				} ) : null;
		} )
		.then( res => {
			return client.indices.exists( {
					index: INDEX_NAME
				} )
				.catch( e => console.log( e ) )
				.then( r => r )
		} )
		.then( r => console.log( r ) );

}
