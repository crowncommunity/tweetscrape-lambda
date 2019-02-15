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
        console.log('deleted', data);
        return data;
    })
}

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

    var queueUrl = await getQueueUrl(context)

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
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(messages)
    }
}
