const AWS = require( 'aws-sdk' );
const isScalar = require('is-scalar');

const DEV_MODE = process.env.IS_LOCAL || process.env.IS_OFFLINE;

const {
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    INDEX_NAME,
    S3_SAVE_BUCKET,
    ELASTIC_HOST,
    ELASTIC_REGION
} = process.env;

const ES = require( 'elasticsearch' );
const AWS_class = require( 'http-aws-es' );

const credentials = DEV_MODE ? new AWS.Credentials(
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
) : new AWS.EnvironmentCredentials( 'AWS' )

const client = ES.Client( {
    hosts: ELASTIC_HOST,
	connectionClass: AWS_class,
	awsConfig: new AWS.Config( {
		region: ELASTIC_REGION,
		credentials: credentials,
	} )
})

const bucket = new AWS.S3( {
	region: ELASTIC_REGION,
	credentials: credentials,
} )

const reserved = ['tweetData','timestamp', 'metadata']
const allowed = ['INSERT', 'REPLACE', 'APPEND', 'REMOVE', 'DELETE'];
const retries = 3;

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


const bucketEvent = async record => {
    console.log('bucket record', record)

    const filename = record.s3.object.key;
    const tweetId = filename.replace(/^(\d+).json/, (m, p1) => {
        if (!p1) {
            throw new Error ('invalid object name');
        }
        return p1;
    })
    const bucket_id = record.s3.bucket.name;

    console.log('item to add to ES', filename, tweetId);

    return bucket.getObject({
        Bucket: bucket_id,
        Key: filename
    })
    .promise()
    .then (file => {
        console.log('s3 file', file);
        if (!file.Body || !file.Body instanceof Buffer) {
            throw new Error ('S3 Bucket error. Body missing from file get');
        }

        let document = JSON.parse(file.Body);
        delete document.parents
        delete document.replies

        /* REMEMBER we've already deleeted nulls in the scrape */

        // console.log(JSON.parse(file.Body));

        return client.update( {
            index: INDEX_NAME,
            type: '_doc',
            id: tweetId,
            retryOnConflict: retries,
        	body: {
        		doc: document,
        		doc_as_upsert: true
        	}
        } )
    })

}

const tableEvent = async record => {
    // console.log('table record', record)
    try {
        if (!record.dynamodb) {
            throw new Error ('invalid record object. missing record');
        }

        var item = record.dynamodb;

        var keys = normalize(item.Keys).normalized;

        if(!keys.TweetId) {
            throw new Error('invalid record object, missing id');
        }

        var data = normalize(item.NewImage).normalized;

        if (allowed.indexOf(data.Action || null) < 0) {
            throw new Error('Invalid Action Type ' + data.Action);
        }

        if (!data.Field) {
            throw new Error('Field must be present')
        }

        if (!data.Field.match(/^\w[\d\w]+$/)) {
            throw new Error('Field name may only contain word characters and digits, and must begin with a word character')
        }

        if (reserved.indexOf(data.Field) > -1) {
            throw new Error('Reserved Field Name Error')
        }
    } catch (e) {
        return { DataError: e }
    }

    return client.get({
        index: INDEX_NAME,
        type: '_doc',
        id: keys.TweetId
    })
    .catch(err => {
        throw err;
    })
    .then (doc => {
        let current = doc._source;

        // delete current.tweetData // we do not modify the tweet data here... ever

        // temporary
        // delete current.tweetData.screenshot

        let m;
        let newData;
        let newField;
        switch(data.Action) {
            case 'INSERT':
            case 'REPLACE':
                try {
                    if (!data.Content) {
                        throw new Error('Content must be present for action type' + data.Action);
                    }
                    m = data.Content.match(/^[\s\r\n]*[\{\[]/);
                    if (m) {
                        data.Content = JSON.parse(data.Content)
                    }
                    if (!data.Content) {
                        throw new Error('Content parse error. must be scalar value or JSON string');
                    }

                    newData = {
                        [data.Field] : data.Content,
                        metadata: {
                            version: data.Timestamp,
                            parent: current.metadata && current.metadata.version || "0",
                            lastUpdatedBy: data.User || 'system',
                        }
                    }

                    console.log('new data', newData);
                } catch (e) {
                    return { DataError: e }
                }

                return client.update( {
                    index: INDEX_NAME,
            	    type: '_doc',
                    id: keys.TweetId,
                    retryOnConflict: retries,
                    body: {
                        doc: newData,
                        doc_as_upsert: true
                    }
                } )
                break;

            case 'APPEND':
                // we /could/ actually foece convert scalar to array... hmmmm
            case 'REMOVE':
                try {
                    if (current[data.Field] && !Array.isArray(current[data.Field])) {
                        throw new Error('Data Error. Data field must be an Array for action type ' + data.Action);
                    }
                    if (!data.Content) {
                        throw new Error('Content must be present for action type' + data.Action);
                    }
                    m = data.Content.match(/^[\s\r\n]*[\{\[]/);
                    if (m) {
                        data.Content = JSON.parse(data.Content)
                    }
                    if (!data.Content) {
                        throw new Error('Content parse error. must be scalar value or JSON string');
                    }

                    if (isScalar(data.Content)) {
                        data.Content = [data.Content];
                    }
                    if (!Array.isArray(data.Content)) {
                        throw new Error ('Content error. must be a scalar value or array');
                    }

                    if (data.Action == 'APPEND') {
                        newField = (current[data.Field] || []).concat(data.Content).filter( (item, index, myself) => {
                            return myself.indexOf(item) >= index;
                        })
                    } else {
                        newField = (current[data.Field] || []).filter((item, index) => {
                            return data.Content.indexOf(item) < 0
                        })
                    }

                    newData = {
                        [data.Field] : newField,
                        metadata: {
                            version: data.Timestamp,
                            parent: current.metadata && current.metadata.version || "0",
                            lastUpdatedBy: data.User || 'system',
                        }
                    }

                    console.log('new data', newData);
                } catch (e) {
                    return { DataError: e }
                }

                return client.update( {
                    index: INDEX_NAME,
            	    type: '_doc',
            	    id: keys.TweetId,
                    retryOnConflict: retries,
            	    body: {
                        doc: newData,
                        doc_as_upsert: true
                    }
                } )
                break;

            case 'DELETE':
                newData = {
                    [data.Field] : null,
                    metadata: {
                        version: data.Timestamp,
                        parent: current.metadata && current.metadata.version || "0",
                        lastUpdatedBy: data.User || 'system',
                    }
                }

                console.log('deleted field', data.Field)
                console.log('new data', newData);

                return client.update( {
                    index: INDEX_NAME,
                    type: '_doc',
                    id: keys.TweetId,
                    retryOnConflict: retries,
                    body: {
                        doc: newData,
                        doc_as_upsert: true,
                    }
                } ).then(() => {
                    return client.update( {
                        index: INDEX_NAME,
                        type: '_doc',
                        id: keys.TweetId,
                        retryOnConflict: retries,
                        body: {
                            script: 'ctx._source.remove("' + data.Field + '")'
                        }
                    })
                })
                break;

            default:
                throw new Error('Unknown Action Type');
        }

    })
            // console.log(data)
            // console.log(current);

// FIELDS
// tweet_id
// timestamp
// parent_ts
// user_id or username
// field_name
// action (insert replace [append remove] delete)
// content


}

module.exports.modify = async ( event, context ) => {
    // console.log(JSON.stringify(event));
    if (event.Records && event.Records.length) {
        console.log('records to process', event.Records.length)

        var errors = [];
        /**
         * WE MIGHT WANT TO FORCE THIS TO OPERATE IN ORDER, RATHER THAN RELY ON PROMISES
         */
        var all = event.Records.map(record => {
            try {
                var res;
                switch (record.eventSource) {
                    case 'aws:s3':
                        res = bucketEvent(record);
                        break;
                    case 'aws:dynamodb':
                        res = (record.eventName == 'INSERT' || record.eventName == 'MODIFY') && tableEvent(record);
                        break;
                    default:
                        console.log('unknown record source', record);
                }
                if (res && !res.DataError) {
                    return res.then(ret => {
                        return {
                            result: 1,
                            return: ret
                        }
                    }).catch (err => {
                        errors.push(err)
                        return {
                            result: -1,
                            message: err.message,
                            error: err
                        }
                    })
                } else if (res.DataError) {
                    errors.push(res.DataError)
                    return {
                        result: -1,
                        message: res.DataError.message,
                        error: res.DataError
                    }
                } else {
                    return {
                        result: 0
                    }
                }
            } catch (e) {
                errors.push(e);
                return {
                    result: -1,
                    message: e.message,
                    error: e
                }
            }
        })

        return await Promise.all( all )
            .then( resp => {
                console.log(resp)
                context.done()
            } )
            .catch( err => {
                errors.push( 'promise error', err )
                console.log( errors );
                // context.fail();
                context.done(); // fail results in repeat attempts.. to allow so requires a lot of analysis of the error(s)
            } )
    }
}
