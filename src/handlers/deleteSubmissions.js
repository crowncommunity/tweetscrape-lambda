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
        return data
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

module.exports.main = async (event, context) => {
    if (event.body) console.log('body', event.body);

    const chunk = (a, s = 10)  => {
        var ret = [];

        for (i = 0 ; i < a.length ; i += s) {
            ret.push(a.slice(i, s));
        }

        return ret;
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

        var queueUrl = await getQueueUrl(context)

        var deletes = await Promise.all(chunks.map(c => {
            return delMsgBatch(queueUrl, c)
        })).then(all => {
            console.log(all);
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
