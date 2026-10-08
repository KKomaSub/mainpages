(async () => {
    'use strict';

    const TAG = '';
    const sleep = ms => new Promise(r => setTimeout(r, ms));

    try { window.__ENTRY_RT_FINAL__?.stop?.(); } catch (_) {}
    try { window.__ENTRY_RT_BRIDGE__?.stop?.(); } catch (_) {}

    const E = (!document.getElementsByTagName("iframe")) ? window.Entry : document.getElementsByTagName("iframe")[0].contentWindow.Entry;
    const vc = E?.variableContainer;

    if (!E || !vc) {
        console.error(TAG, 'Entry가 준비되지 않았습니다.');
        return;
    }

    const projectId =
        E.projectId ||
        E.project?._id ||
        E.project?.id ||
        E.options?.projectId ||
        location.pathname.match(/[0-9a-f]{24}/i)?.[0] ||
        location.href.match(/[0-9a-f]{24}/i)?.[0];

    if (!projectId) {
        console.error(TAG, '작품 ID를 찾지 못했습니다.');
        return;
    }

    const nativeCV =
        E.cloudVariable ||
        vc.cloudVariable ||
        window.__RT_NULL_FIX__?.cloudVariable ||
        window.__ENTRY_RT_NATIVE__?.cloudVariable;

    const oldType =
        nativeCV?.cvServer?.type;

    async function getCsrf() {
        let token =
            document.querySelector(
                'meta[name="csrf-token"]'
            )?.content;

        if (token)
            return token;

        try {
            const r = await fetch('/ws/new', {
                credentials: 'include'
            });

            const html = await r.text();

            token =
                html.match(
                    /name=["']csrf-token["'][^>]*content=["']([^"']+)/i
                )?.[1] ||
                html.match(
                    /content=["']([^"']+)["'][^>]*name=["']csrf-token/i
                )?.[1];
        } catch (_) {}

        return token || '';
    }

    async function freshServerInfo() {
        const csrf = await getCsrf();

        const headers = {
            'content-type': 'application/json'
        };

        if (csrf) {
            headers['csrf-token'] = csrf;
            headers['x-csrf-token'] = csrf;
        }

        const payload = {
            query: `
                query GET_CLOUD_SERVER_INFO($id: ID!) {
                    cloudServerInfo(id: $id) {
                        url
                        query
                    }
                }
            `,
            variables: {
                id: projectId
            }
        };

        const endpoints = [
            '/graphql/GET_CLOUD_SERVER_INFO',
            '/graphql'
        ];

        let lastError;

        for (const endpoint of endpoints) {
            try {
                const response = await fetch(endpoint, {
                    method: 'POST',
                    credentials: 'include',
                    headers,
                    body: JSON.stringify(payload)
                });

                const json = await response.json();

                const info =
                    json?.data?.cloudServerInfo;

                if (
                    response.ok &&
                    info?.url &&
                    info?.query
                ) {
                    return info;
                }

                lastError =
                    json?.errors?.[0]?.message;
            } catch (e) {
                lastError = e;
            }
        }

        throw new Error(
            String(
                lastError ||
                'cloudServerInfo 발급 실패'
            )
        );
    }

    const OMIT = Symbol('omit');

    function createClient(info, type) {
        let ws;
        let ackId = 1;

        const ackMap = new Map();
        const listeners = new Map();

        let resolveWelcome;
        let rejectWelcome;

        const welcomePromise =
            new Promise((resolve, reject) => {
                resolveWelcome = resolve;
                rejectWelcome = reject;
            });

        function fire(name, ...args) {
            const set = listeners.get(name);

            if (!set)
                return;

            for (const fn of [...set]) {
                try {
                    fn(...args);
                } catch (_) {}
            }
        }

        function on(name, fn) {
            if (!listeners.has(name))
                listeners.set(name, new Set());

            listeners.get(name).add(fn);

            return () =>
                listeners.get(name)?.delete(fn);
        }

        function url() {
            const u = new URL(
                info.url || location.origin,
                location.origin
            );

            u.protocol =
                u.protocol === 'http:'
                    ? 'ws:'
                    : 'wss:';

            u.pathname = '/cv/';
            u.search = '';

            if (type !== OMIT) {
                u.searchParams.set(
                    'type',
                    String(type)
                );
            }

            u.searchParams.set(
                'q',
                info.query
            );

            u.searchParams.set('EIO', '3');
            u.searchParams.set(
                'transport',
                'websocket'
            );

            return u.toString();
        }

        function sendRaw(s) {
            if (
                ws?.readyState ===
                WebSocket.OPEN
            ) {
                ws.send(s);
                return true;
            }

            return false;
        }

        function emit(name, ...args) {
            return sendRaw(
                '42' +
                JSON.stringify([
                    name,
                    ...args
                ])
            );
        }

        function emitAck(
            name,
            args,
            timeout = 2500
        ) {
            return new Promise(resolve => {
                if (
                    ws?.readyState !==
                    WebSocket.OPEN
                ) {
                    resolve(null);
                    return;
                }

                const id = ackId++;

                const timer =
                    setTimeout(() => {
                        ackMap.delete(id);
                        resolve(null);
                    }, timeout);

                ackMap.set(id, payload => {
                    clearTimeout(timer);
                    resolve(payload);
                });

                ws.send(
                    '42' +
                    id +
                    JSON.stringify([
                        name,
                        ...args
                    ])
                );
            });
        }

        function parseEvent(packet) {
            const p = packet.indexOf('[');

            if (p < 0)
                return;

            try {
                const arr =
                    JSON.parse(
                        packet.slice(p)
                    );

                fire(
                    arr[0],
                    ...arr.slice(1)
                );
            } catch (_) {}
        }

        function parseAck(packet) {
            const m =
                packet.match(/^43(\d+)(.*)$/);

            if (!m)
                return;

            const id = Number(m[1]);

            let payload = [];

            try {
                payload =
                    JSON.parse(
                        m[2] || '[]'
                    );
            } catch (_) {}

            const fn = ackMap.get(id);

            if (fn) {
                ackMap.delete(id);
                fn(payload);
            }
        }

        ws = new WebSocket(url());

        ws.addEventListener(
            'message',
            e => {
                const msg =
                    String(e.data);

                if (msg.startsWith('0')) {
                    sendRaw('40');
                    return;
                }

                if (msg === '2') {
                    sendRaw('3');
                    return;
                }

                if (msg.startsWith('43')) {
                    parseAck(msg);
                    return;
                }

                if (msg.startsWith('42')) {
                    parseEvent(msg);
                    return;
                }
            }
        );

        on('check', id => {
            emit('imAlive', id);
        });

        on('welcome', data => {
            resolveWelcome(data || {});
        });

        ws.addEventListener(
            'error',
            e => {
                fire('socketError', e);
            }
        );

        ws.addEventListener(
            'close',
            e => {
                fire('close', e);
            }
        );

        const timeout =
            setTimeout(() => {
                rejectWelcome(
                    new Error(
                        'welcome timeout'
                    )
                );
            }, 7000);

        const ready =
            welcomePromise.finally(() =>
                clearTimeout(timeout)
            );

        return {
            ws,
            ready,
            on,
            emit,
            emitAck,

            close() {
                try {
                    ws.close();
                } catch (_) {}
            }
        };
    }

    const types = [];

    function addType(v) {
        const key =
            v === OMIT
                ? '__OMIT__'
                : String(v);

        if (
            !types.some(
                x =>
                    (
                        x === OMIT
                            ? '__OMIT__'
                            : String(x)
                    ) === key
            )
        ) {
            types.push(v);
        }
    }

    if (oldType !== undefined)
        addType(oldType);

    addType('undefined');

    if (E.type)
        addType(E.type);

    addType('project');
    addType('play');
    addType('workspace');
    addType(OMIT);

    async function testBroadcast(
        a,
        b,
        welcomeA
    ) {
        const variables =
            welcomeA?.variables || [];

        const probe =
            variables.find(
                v =>
                    v?.variableType ===
                        'variable' &&
                    v.id &&
                    v._id
            );

        if (!probe)
            return false;

        return new Promise(
            async resolve => {
                let done = false;

                const finish =
                    value => {
                        if (done)
                            return;

                        done = true;
                        off();
                        clearTimeout(timer);
                        resolve(value);
                    };

                const off =
                    b.on(
                        'action',
                        operation => {
                            if (
                                operation?.id ===
                                    probe.id &&
                                operation?.type ===
                                    'set'
                            ) {
                                finish(true);
                            }
                        }
                    );

                const timer =
                    setTimeout(
                        () =>
                            finish(false),
                        1400
                    );

                const ack =
                    await a.emitAck(
                        'action',
                        [{
                            type: 'set',
                            _id: probe._id,
                            id: probe.id,
                            value: probe.value,
                            variableType:
                                'variable'
                        }],
                        1200
                    );

                if (
                    Array.isArray(ack) &&
                    ack[0] === false
                ) {
                    finish(false);
                }
            }
        );
    }

    async function tryMode(type) {
        const [
            infoA,
            infoB
        ] = await Promise.all([
            freshServerInfo(),
            freshServerInfo()
        ]);

        const a =
            createClient(infoA, type);

        const b =
            createClient(infoB, type);

        let wa, wb;

        try {
            [wa, wb] =
                await Promise.all([
                    a.ready,
                    b.ready
                ]);
        } catch (e) {
            a.close();
            b.close();
            return null;
        }

        if (
            await testBroadcast(
                a,
                b,
                wa
            )
        ) {
            return {
                a,
                b,
                welcome: wa,
                type,
                target: null
            };
        }

        const targets = [];

        function addTarget(x) {
            if (
                x === undefined ||
                x === null
            )
                return;

            const s = String(x);

            if (!targets.includes(s))
                targets.push(s);
        }

        addTarget(projectId);
        addTarget(oldType);
        addTarget(E.type);

        if (type !== OMIT)
            addTarget(type);

        for (
            const target of
            targets.slice(0, 4)
        ) {
            a.emit(
                'changeMode',
                'online',
                target
            );

            b.emit(
                'changeMode',
                'online',
                target
            );

            await sleep(180);

            if (
                await testBroadcast(
                    a,
                    b,
                    wa
                )
            ) {
                return {
                    a,
                    b,
                    welcome: wa,
                    type,
                    target
                };
            }
        }

        a.close();
        b.close();

        return null;
    }

    let winner = null;

    for (const type of types) {
        try {
            winner =
                await tryMode(type);

            if (winner)
                break;
        } catch (e) {
        }
    }

    if (!winner) {
        console.error(
            TAG,
            '모든 /cv 모드에서 교차 클라이언트 브로드캐스트가 확인되지 않았습니다.'
        );

        console.error(
            TAG,
            '단순 WebSocket 문제는 아닙니다. 서버가 현재 작품/세션의 실시간 쓰기를 offline 처리하고 있습니다.'
        );

        return;
    }

    winner.b.close();

    let primary =
        winner.a;

    let currentWelcome =
        winner.welcome;

    const state =
        new Map();

    function normalizeList(v) {
        if (
            Array.isArray(v?.array)
        ) {
            return v.array.map(x => ({
                key:
                    x?.key ||
                    x?._key,
                data:
                    x?.data
            }));
        }

        if (
            Array.isArray(v?.list) &&
            v?.value &&
            typeof v.value ===
                'object'
        ) {
            return v.list.map(key => ({
                key,
                data: v.value[key]
            }));
        }

        return [];
    }

    function importWelcome(welcome) {
        state.clear();

        for (
            const v of
            welcome?.variables || []
        ) {
            if (
                v.variableType ===
                'list'
            ) {
                state.set(
                    v.id,
                    {
                        ...v,
                        array:
                            normalizeList(v)
                    }
                );
            } else {
                state.set(
                    v.id,
                    { ...v }
                );
            }
        }
    }

    importWelcome(currentWelcome);

    function updateEntry(record) {
        if (!record?.id)
            return;

        if (
            record.variableType ===
                'variable' ||
            record.variableType ===
                'slide'
        ) {
            const v =
                vc.getVariable?.(
                    record.id
                );

            if (v) {
                v.value_ =
                    record.value;

                v._valueWidth = null;

                try {
                    v.updateView?.();
                } catch (_) {}
            }
        }

        if (
            record.variableType ===
                'list'
        ) {
            const list =
                vc.getList?.(
                    record.id
                );

            if (list) {
                list.array_ =
                    (record.array || [])
                        .map(x => ({
                            data: x.data
                        }));

                try {
                    list.updateView?.();
                } catch (_) {}
            }
        }

        E.requestUpdateTwice = true;
    }

    function applyAction(op) {
        if (!op?.id)
            return;

        let record =
            state.get(op.id);

        if (!record) {
            record = {
                id: op.id,
                _id: op._id,
                variableType:
                    op.variableType
            };

            if (
                op.variableType ===
                'list'
            ) {
                record.array = [];
            }

            state.set(
                op.id,
                record
            );
        }

        if (
            op.variableType ===
                'variable' ||
            op.variableType ===
                'slide'
        ) {
            if (
                op.type === 'set'
            ) {
                record.value =
                    op.value !== undefined
                        ? op.value
                        : op.data;
            }
        }

        if (
            op.variableType ===
                'list'
        ) {
            const arr =
                record.array ||
                (record.array = []);

            switch (op.type) {
                case 'append':
                    arr.push({
                        key:
                            op.key ||
                            op.newKey,
                        data: op.data
                    });
                    break;

                case 'insert':
                    arr.splice(
                        Number(op.index) || 0,
                        0,
                        {
                            key:
                                op.key ||
                                op.newKey,
                            data:
                                op.data
                        }
                    );
                    break;

                case 'delete': {
                    let i =
                        arr.findIndex(
                            x =>
                                x.key ===
                                op.key
                        );

                    if (
                        i < 0 &&
                        Number.isInteger(
                            op.index
                        )
                    ) {
                        i = op.index;
                    }

                    if (i >= 0)
                        arr.splice(i, 1);

                    break;
                }

                case 'replace': {
                    let i =
                        arr.findIndex(
                            x =>
                                x.key ===
                                op.key
                        );

                    if (
                        i < 0 &&
                        Number.isInteger(
                            op.index
                        )
                    ) {
                        i = op.index;
                    }

                    if (i >= 0) {
                        arr[i] = {
                            key:
                                op.newKey ||
                                arr[i].key,
                            data:
                                op.data
                        };
                    }

                    break;
                }
            }
        }

        updateEntry(record);
    }

    function attachPrimary(client) {
        client.on(
            'action',
            applyAction
        );

        client.on(
            'reset',
            variables => {
                importWelcome({
                    variables
                });

                for (
                    const r of
                    state.values()
                ) {
                    updateEntry(r);
                }
            }
        );
    }

    attachPrimary(primary);

    let reconnecting = null;
    let stopped = false;

    async function reconnect() {
        if (stopped)
            throw new Error('stopped');

        if (
            primary?.ws?.readyState ===
            WebSocket.OPEN
        ) {
            return primary;
        }

        if (reconnecting)
            return reconnecting;

        reconnecting =
            (async () => {
                const info =
                    await freshServerInfo();

                const next =
                    createClient(
                        info,
                        winner.type
                    );

                const welcome =
                    await next.ready;

                if (winner.target) {
                    next.emit(
                        'changeMode',
                        'online',
                        winner.target
                    );
                }

                importWelcome(welcome);

                attachPrimary(next);

                primary = next;

                return next;
            })().finally(() => {
                reconnecting = null;
            });

        return reconnecting;
    }

    async function sendOperation(
        operation
    ) {
        const client =
            await reconnect();

        const ack =
            await client.emitAck(
                'action',
                [operation],
                3000
            );

        if (
            !Array.isArray(ack) ||
            ack[0] !== true
        ) {
            throw new Error(
                '실시간 변수 서버가 action을 거절했습니다.'
            );
        }

        const normalized =
            ack[1] || operation;

        applyAction(normalized);

        return normalized;
    }

    const shim = {
        async connect() {
            await reconnect();
        },

        get(target) {
            return state.get(
                target?.id
            );
        },

        async set(target, value) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            return sendOperation({
                _id: v._id,
                id: v.id,
                variableType:
                    target.variableType ||
                    v.variableType ||
                    'variable',
                type: 'set',
                value
            });
        },

        async append(target, data) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            return sendOperation({
                _id: v._id,
                id: v.id,
                variableType: 'list',
                type: 'append',
                data
            });
        },

        async insert(
            target,
            index,
            data
        ) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            return sendOperation({
                _id: v._id,
                id: v.id,
                variableType: 'list',
                type: 'insert',
                index,
                data
            });
        },

        async delete(
            target,
            index
        ) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            const item =
                v.array?.[index];

            return sendOperation({
                _id: v._id,
                id: v.id,
                variableType: 'list',
                type: 'delete',
                index,
                key: item?.key
            });
        },

        async replace(
            target,
            index,
            data
        ) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            const item =
                v.array?.[index];

            return sendOperation({
                _id: v._id,
                id: v.id,
                variableType: 'list',
                type: 'replace',
                index,
                key: item?.key,
                data
            });
        },

        setArray(target, array) {
            const v =
                state.get(target?.id);

            if (!v)
                return;

            v.array =
                (array || []).map(
                    (x, i) => ({
                        key:
                            v.array?.[i]?.key,
                        data:
                            x?.data ?? x
                    })
                );
        },

        enable(target) {
            reconnect().then(client =>
                client.emit(
                    'changeMode',
                    'online',
                    target ||
                    winner.target ||
                    projectId
                )
            );
        },

        disable() {}
    };

    let bound = 0;

    function bindItem(v) {
        if (
            !v ||
            !v.isRealTime_
        )
            return;

        v.cloudVariable = shim;
        bound++;
    }

    function bindArray(arr) {
        if (!Array.isArray(arr))
            return;

        arr.forEach(bindItem);
    }

    function bindEverything() {
        E.cloudVariable = shim;
        vc.cloudVariable = shim;

        bindArray(vc.variables_);
        bindArray(vc.lists_);

        for (
            const object of
            E.container?.objects_ || []
        ) {
            bindArray(
                object?.entity?.variables
            );

            bindArray(
                object?.entity?.lists
            );

            for (
                const clone of
                object?.clonedEntities || []
            ) {
                bindArray(
                    clone?.variables
                );

                bindArray(
                    clone?.lists
                );
            }
        }
    }

    bindEverything();

    const EP =
        E.EntryObject?.prototype;

    if (
        EP &&
        typeof EP.addCloneVariables ===
            'function' &&
        !EP.__RT_FINAL_PATCH__
    ) {
        const old =
            EP.addCloneVariables;

        EP.addCloneVariables =
            function(...args) {
                const r =
                    old.apply(
                        this,
                        args
                    );

                const entity =
                    args[1];

                bindArray(
                    entity?.variables
                );

                bindArray(
                    entity?.lists
                );

                return r;
            };

        EP.__RT_FINAL_PATCH__ = true;
    }

    for (
        const r of
        state.values()
    ) {
        updateEntry(r);
    }

    const repairTimer =
        setInterval(() => {
            if (!stopped)
                bindEverything();
        }, 1000);

    window.__ENTRY_RT_FINAL__ = {
        shim,

        get connected() {
            return (
                primary?.ws?.readyState ===
                WebSocket.OPEN
            );
        },

        type: winner.type,
        target: winner.target,

        state,

        async reconnect() {
            try {
                primary?.close?.();
            } catch (_) {}

            return reconnect();
        },

        stop() {
            stopped = true;

            clearInterval(
                repairTimer
            );

            try {
                primary?.close?.();
            } catch (_) {}
        }
    };
    console.log("복구완료!")
})();
