// Opaque serialized data (MessagePack header and frames)
type Serialized = {};

// Binary MessagePack array
type Bytes = [number];


// Global types
type WorkerMetrics = {
    "bandwidth": {
        "total": number,
        "types": {},
        "workers": {}
    },
    "cpu": number,
    "executing": number,
    "in_flight": number,
    "in_memory": number,
    "memory": number,
    "num_fds": number,
    "read_bytes": number,
    "ready": number,
    "time": number,
    "write_bytes": number
};

type TaskDef = Serialized | {
    "func": Bytes | Serialized,
    "args": Bytes | Serialized,
    "kwargs": Bytes | Serialized
};

type GetDataResponseType = {
    "data": { [key: string]: Serialized },
    "status": "OK"
};


/**
 * UNINITIATED MESSAGES
 *
 * The following messages are sent on a connection that was not initiated with RegisterClient/RegisterWorker yet.
 */

//---------------------//
// CLIENT -> SCHEDULER //
//---------------------//
type IdentityMsg = {
    "op": "identity",
    "reply": boolean
};
type RegisterClientMsg = {
    "op": "register-client",
    "client": string,
    "reply": boolean
};
type GatherMsg = {
    "op": "gather",
    "keys": [string],
    "reply": boolean
};
type NcoresMsg = {
    "op": "ncores",
    "reply": boolean,
    "workers": {}
};


//---------------------//
// SCHEDULER -> CLIENT //
//---------------------//
type IdentityResponseMsg = {
    "op": "identity-response",
    "type": "Scheduler",
    "id": number,
    "workers": { [address: string]: {
        'host': string,
        'id': string,
        'last_seen': number,
        'local_directory': string,
        'memory_limit': number,
        'metrics': WorkerMetrics,
        'name': string,
        'nanny': string,
        'nthreads': number,
        'resources': {},
        'services': {'dashboard': number},
        'type': 'Worker'
    }}
};
/**
 * This message must be inside a message array and the array must have length 1!!!
 */
type RegisterClientResponseMsg = {
    "op": "stream-start"
};
type NcoresResponseMsg = {
    [worker: string]: number
};
type SchedulerGetDataResponse = GetDataResponseType;


//---------------------//
// WORKER -> SCHEDULER //
//---------------------//
type HeartbeatWorkerMsg = {
    "op": "heartbeat_worker",
    "reply": boolean,
    "now": number,
    "address": string,
    "metrics": WorkerMetrics
};
type RegisterWorkerMsg = {
    'op': 'register-worker',
    'address': string,
    'extra': {},
    'keys': [string],
    'local_directory': string,
    'memory_limit': number,
    'metrics': WorkerMetrics,
    'name': string,
    'nanny': string,
    'nbytes': {},
    'now': number,
    'nthreads': number,
    'pid': number,
    'reply': boolean,
    'resources': {},
    'services': {'dashboard': number},
    'types': {}
};
type WorkerGetDataResponse = GetDataResponseType;


//---------------------//
// SCHEDULER -> WORKER //
//---------------------//
type RegisterWorkerResponseMsg = {
    status: String,
    time: number,
    heartbeat_interval: number,
    worker_plugins: [string]
};
type GetDataMsg = {
    "op": "get_data",
    "keys": [string],
    "reply?": boolean,
    "report?": boolean,
    "who?": string,
    "max_connections?": number | boolean
};
type GetDataResponseConfirm = "OK";


/**
 * INITIATED MESSAGES
 *
 * The following messages are sent after RegisterClient/RegisterWorker.
 */

//---------------------//
// CLIENT -> SCHEDULER //
//---------------------//
type HeartbeatClientMsg = {
    "op": "heartbeat-client"
};
type UpdateGraphMsg = {
    "op": "update-graph",
    "keys": [string],
    "dependencies": { [key: string]: [string] },
    "tasks": { [key: string]: TaskDef },
    "priority": { [key: string]: number },
    "user_priority": number,
    "fifo_timeout": string,
    "loose_restrictions": null,
    "restrictions": {},
    "retries": null,
    "submitting_task": null,
    "resources": null
};
type ReleaseKeysMsg = {
    "op": "client-releases-keys",
    "keys": [string],
    "client": string
};
type CloseClientMsg = {
    "op": "close-client"
};
type CloseStreamMsg = {
    "op": "close-stream"
};


//---------------------//
// SCHEDULER -> CLIENT //
//---------------------//
type KeyInMemoryMsg = {
    "op": "key-in-memory",
    "key": string,
    "type": Bytes
};
type ClientTaskErredMsg = {
    "op": "task-erred",
    "key": string,
    "exception": Serialized,
    "traceback": Serialized
};


//---------------------//
// SCHEDULER -> WORKER //
//---------------------//
/**
 * Either `task` is present OR `func` + `args` + (optionally) `kwargs` is present.
 */
type ComputeTaskMsg = {
    "op": "compute-task",
    "key": string,
    "duration": number,
    "priority": [number],
    "nbytes?": { [task: string]: number },
    "who_has?": { [task: string]: string },
    "task?": Serialized,
    "func?": Bytes | Serialized,
    "args?": Bytes | Serialized,
    "kwargs?": Bytes | Serialized
};


//---------------------//
// WORKER -> SCHEDULER //
//---------------------//
type TaskFinishedMsg = {
    "op": "task-finished",
    "key": string,
    "nbytes": number,
    "status": "OK",
    "startstops": [{"action": string, "start": number, "stop": number}],
    "thread": number,
    "type": Bytes,
    "typename": string
};
type WorkerTaskErredMsg = {
    "op": "task-erred",
    "key": string,
    "status": "error",
    "exception": Serialized,
    "traceback": Serialized,
    "thread": number
};
type KeepAliveMsg = {
    "op": "keep-alive",
};