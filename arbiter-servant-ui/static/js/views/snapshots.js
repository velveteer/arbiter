// The DLQ and the archive: tables of job snapshots, one component over two configs.
import { reactive, shallowRef, computed, provide, inject } from '../../vendor/vue.esm-browser.prod.js';
import { api } from '../api.js';
import { formatJson, parsePayload, pluralize } from '../format.js';
import { toast } from '../store.js';
import { ancestors, fillAncestors, submitForm, useArm, useBusy, useColumns, useDetail, useTable, useView } from '../use.js';

const blankEdit = () => ({ id: null, text: '', error: '', saving: false });

const DLQ = {
  tab: 'dlq',
  one: 'DLQ entry',
  many: 'DLQ entries',
  id: 'dlqPrimaryKey',
  title: 'DLQ entry',
  idLabel: 'DLQ ID',
  cols: [
    { key: 'select', label: '', weight: 4, required: true, narrow: false },
    { key: 'id', was: 'dlqid', label: 'DLQ ID', weight: 7, narrow: false, sort: 'id' },
    { key: 'jobid', label: 'Job ID', weight: 7, sort: 'job_id' },
    { key: 'parent', label: 'Parent', weight: 7, narrow: false, sort: 'parent_id' },
    { key: 'group', label: 'Group', weight: 8, narrow: false, sort: 'group_key' },
    { key: 'kind', label: 'Kind', weight: 8, autoHide: true, narrow: false },
    { key: 'payload', label: 'Payload', weight: 13 },
    { key: 'failed', label: 'Failed', weight: 12, sort: 'failed_at' },
    { key: 'attempts', label: 'Attempts', weight: 8, narrow: false, sort: 'attempts' },
    { key: 'error', label: 'Last Error', weight: 13, narrow: false },
    { key: 'gates', label: 'Gates', weight: 13, autoHide: true, narrow: false },
    { key: 'actions', label: 'Actions', weight: 6, cls: 'cell-actions' },
  ],
  colsKey: 'arb.dlqCols.v2',
  filters: ['group_key', 'parent_id', 'job_id', 'kind', 'payload', 'error'],
  sorts: ['id', 'failed_at', 'job_id', 'priority', 'attempts', 'inserted_at', 'group_key', 'parent_id', 'last_attempted_at'],
  pageKey: 'arb.dlqPageSize',
  refreshKey: 'arb.dlqRefresh',
  list: api.dlq,
  events: (batch, queue) => batch.filter((e) => e.table === queue && e.dlq).length,
  send: api.retryDlq,
  // A retry restores the entry's whole DLQ tree.
  sendsTree: true,
  sendLabel: 'Retry',
  sendConfirm: '',
  sendDone: 'Retried',
  editLabel: 'Edit and retry',
  del: api.deleteDlq,
  delMany: api.deleteDlqMany,
  delLabel: 'Delete',
  delConfirm: 'Confirm delete permanently',
  delDone: 'Deleted',
  bulkSend: { label: 'Retry selected', confirm: 'Confirm retry ' },
  bulkDel: { label: 'Delete selected', confirm: 'Confirm delete ', suffix: ' permanently' },
  empty: 'No DLQ entries.',
};

const ARCHIVE = {
  tab: 'archive',
  one: 'archived job',
  many: 'archived jobs',
  id: 'archivePrimaryKey',
  title: 'Archived job',
  idLabel: 'Archive ID',
  cols: [
    { key: 'select', label: '', weight: 4, required: true, narrow: false },
    { key: 'id', was: 'archiveid', label: 'Archive ID', weight: 6, narrow: false, sort: 'id' },
    { key: 'jobid', label: 'Job ID', weight: 6, sort: 'job_id' },
    { key: 'parent', label: 'Parent', weight: 6, narrow: false, sort: 'parent_id' },
    { key: 'group', label: 'Group', weight: 8, narrow: false, sort: 'group_key' },
    { key: 'kind', label: 'Kind', weight: 8, autoHide: true, narrow: false },
    { key: 'payload', label: 'Payload', weight: 18 },
    { key: 'result', was: 'hasresult', label: 'Result', weight: 7, narrow: false },
    { key: 'inserted', label: 'Inserted', weight: 12, narrow: false, sort: 'inserted_at' },
    { key: 'completed', label: 'Completed', weight: 12, sort: 'completed_at' },
    { key: 'attempts', label: 'Attempts', weight: 8, narrow: false, sort: 'attempts' },
    { key: 'actions', label: 'Actions', weight: 5, cls: 'cell-actions' },
  ],
  colsKey: 'arb.archiveCols',
  // The completion window finds rows in an archive that holds months of jobs.
  filters: ['group_key', 'parent_id', 'job_id', 'kind', 'payload', 'completed_after', 'completed_before'],
  sorts: ['id', 'completed_at', 'inserted_at', 'job_id', 'attempts', 'group_key', 'parent_id'],
  pageKey: 'arb.archivePageSize',
  refreshKey: 'arb.archiveRefresh',
  list: api.archive,
  send: api.requeueArchive,
  sendLabel: 'Re-enqueue',
  sendConfirm: 'Confirm re-enqueue',
  sendDone: 'Re-enqueued',
  editLabel: 'Edit and re-enqueue',
  del: api.deleteArchive,
  delMany: api.deleteArchiveMany,
  delLabel: 'Purge',
  delConfirm: 'Confirm purge',
  delDone: 'Purged',
  bulkSend: { label: 'Re-enqueue selected', confirm: 'Confirm re-enqueue ' },
  bulkDel: { label: 'Purge selected', confirm: 'Confirm purge ', suffix: '' },
  empty: 'No archived jobs.',
};

// Row actions, shared by the row menu and the drawer header.
const SnapshotMenu = {
  props: { row: Object, close: Function },
  setup: () => inject('snapshots'),
  template: /* html */ `
    <button v-if="!cfg.sendConfirm && edit.id !== id(row)" type="button" class="dropdown-item" @click="send(row); close()">{{ cfg.sendLabel }}</button>
    <button v-if="edit.id == null" type="button" class="dropdown-item" @click="openEdit(row); close()">{{ cfg.editLabel }}</button>
    <arm-button v-if="cfg.sendConfirm && edit.id !== id(row)" :arm="arm" :k="'send:' + id(row)" cls="dropdown-item" on="fw-semibold"
      :label="cfg.sendLabel" :confirm="cfg.sendConfirm" @fire="send(row); close()"/>
    <arm-button :arm="arm" :k="'del:' + id(row)" cls="dropdown-item text-danger" on="fw-semibold"
      :label="cfg.delLabel" :confirm="cfg.delConfirm" @fire="del(row); close()"/>`,
};

function snapshotTab(cfg) {
  return {
    components: { SnapshotMenu },
    props: { queue: String },
    setup(props) {
      const queue = props.queue;
      const rows = shallowRef(/** @type {any[]} */ ([]));
      const id = (row) => row[cfg.id];
      const edit = reactive(blankEdit());
      const arm = useArm();
      const busy = useBusy();
      const cols = useColumns(cfg.cols, cfg.colsKey);
      const view = useView({
        noun: cfg.many,
        key: cfg.refreshKey,
        load,
        empty: () => rows.value.length === 0,
        events: cfg.events && ((batch) => cfg.events(batch, queue)),
      });
      const t = useTable({
        view,
        tab: cfg.tab,
        filters: cfg.filters,
        sorts: cfg.sorts,
        pageKey: cfg.pageKey,
        ids: () => rows.value.map(id),
      });
      const cancelEdit = () => Object.assign(edit, blankEdit());
      const d = useDetail({
        rows: () => rows.value,
        id,
        held: () => edit.id != null,
        reset: cancelEdit,
      });

      async function load(stale) {
        const data = await cfg.list(queue, t.params());
        if (stale()) return;
        rows.value = data.items;
        const snaps = rows.value.map((r) => r.jobSnapshot);
        if (snaps.length) {
          cols.measure({ kind: snaps.every((j) => !j.kind), gates: snaps.every((j) => !j.rateLimit && !j.concurrency) });
        }
        // A newer failure can push the open entry to another page, so a row that is not on the page keeps its drawer.
        d.resync();
        t.landed(data.total);
      }

      // A tree send takes the open entry with it when the entry is in the same tree.
      async function closeGone() {
        const cur = d.cur;
        if (!cfg.sendsTree || !d.open || !cur) return;
        const left = await cfg.list(queue, { job_id: cur.jobSnapshot.primaryKey, limit: 1 }).then(
          (r) => r.items,
          () => [cur],
        );
        if (!left.some((r) => id(r) === id(cur))) d.closeIf(id(cur));
      }

      const send = (row, payload) =>
        busy.run(
          id(row),
          async () => {
            await cfg.send(queue, id(row), payload);
            d.closeIf(id(row));
            toast(cfg.sendDone, 'success');
            await Promise.all([view.reload(), closeGone()]);
          },
          `Failed to ${cfg.sendLabel.toLowerCase()}`,
        );

      const del = (row) =>
        busy.run(
          id(row),
          async () => {
            await cfg.del(queue, id(row));
            d.closeIf(id(row));
            await view.reload();
          },
          `Failed to ${cfg.delLabel.toLowerCase()}`,
        );

      async function delMany() {
        const ids = [...t.sel];
        if (!ids.length) return;
        await t.bulk(async (moved) => {
          const n = (await cfg.delMany(queue, ids)).deleted;
          if (moved()) return;
          ids.forEach((x) => d.closeIf(x));
          t.sel.clear();
          view.refresh();
          if (n < ids.length) toast(`${cfg.delDone} ${n} of ${ids.length}. ${ids.length - n} no longer present`, 'warning');
          else toast(`${cfg.delDone} ${n} ${pluralize(n, cfg.one, cfg.many)}`, 'success');
        }, `Failed to ${cfg.delLabel.toLowerCase()}`);
      }

      // The payload editor lives in the drawer, so open the drawer on the row first.
      function openEdit(row) {
        if (!d.view(row)) return;
        Object.assign(edit, blankEdit(), { id: id(row), text: formatJson(row.jobSnapshot?.payload) });
      }

      const editInvalid = computed(() => !edit.text.trim() || !!parsePayload(edit.text.trim()).error);

      function submitEdit() {
        const done = edit.id;
        if (done == null || edit.saving || busy.has(done)) return;
        const parsed = parsePayload(edit.text.trim());
        if (parsed.error) edit.error = 'Invalid JSON: ' + parsed.error;
        if (editInvalid.value) return;
        // A 400 means the queue refuses the payload. Its body says why.
        return submitForm(
          edit,
          async () => {
            await busy.run(done, () => cfg.send(queue, done, parsed.value));
            cancelEdit();
            toast(cfg.sendDone + ' with the changed payload', 'success');
            d.closeIf(done);
            await view.reload();
          },
          (e) => (e.status === 400 && e.body ? e.body : e.message),
        );
      }

      // One request per DLQ tree covers the tree's other selected entries.
      async function byTree(all) {
        if (all.length < 2) return null;
        const byId = new Map(rows.value.map((r) => [id(r), r]));
        const byJob = new Map(rows.value.map((r) => [String(r.jobSnapshot.primaryKey), r]));
        const picked = all.map((x) => byId.get(x));
        const parentOf = (r) => r.jobSnapshot.parentId;
        await fillAncestors(byJob, picked, parentOf, (up) => cfg.list(queue, { job_id: up, limit: 1 }).then((r) => r.items[0]));
        const root = (r) => [r, ...ancestors(byJob, r, parentOf)].at(-1).jobSnapshot.primaryKey;
        const first = new Map();
        const via = new Map();
        picked.forEach((r, i) => {
          if (!r) return;
          const top = root(r);
          if (!first.has(top)) first.set(top, all[i]);
          via.set(all[i], first.get(top));
        });
        return (x) => via.get(x) ?? x;
      }

      const ctx = {
        cfg,
        queue,
        rows,
        t,
        view,
        cols,
        d,
        arm,
        busy,
        edit,
        editInvalid,
        id,
        send,
        del,
        delMany,
        openEdit,
        cancelEdit,
        submitEdit,
        sendMany: async () => {
          await t.each(
            async (x) => {
              await cfg.send(queue, x);
              d.closeIf(x);
            },
            { done: cfg.sendDone, one: cfg.one, many: cfg.many, prepare: cfg.sendsTree ? byTree : undefined },
          );
          await closeGone();
        },
      };
      provide('snapshots', ctx);
      return ctx;
    },
    template: /* html */ `
      <load-error :view="view" :t="t"/>
      <div class="toolbar" v-show="view.ready">
        <refresh-control :view="view"/>
        <filter-builder :t="t"/>
        <columns-menu :cols="cols"/>
      </div>
      <pending-note :view="view"/>

      <bulk-bar :t="t">
        <arm-button :arm="arm" k="bulkSend" on="btn-success" off="btn-outline-success" :disabled="t.busy" :label="cfg.bulkSend.label"
          :confirm="cfg.bulkSend.confirm + t.sel.size" @fire="sendMany"/>
        <arm-button :arm="arm" k="bulkDelete" on="btn-danger" off="btn-outline-danger" :disabled="t.busy" :label="cfg.bulkDel.label"
          :confirm="cfg.bulkDel.confirm + t.sel.size + cfg.bulkDel.suffix" @fire="delMany"/>
      </bulk-bar>

      <pager :t="t" :view="view" :one="cfg.one" :many="cfg.many"/>
      <div v-scroll-edges class="table-responsive" :aria-busy="view.loading" v-show="view.ready">
        <table class="table table-striped table-hover table-sm sticky-head table-fixed">
          <t-head :cols="cols.shown" :sort="t.sort" :table="t"/>
          <tbody>
            <tr v-for="row in rows" :key="id(row)" class="detail-row" tabindex="0" :class="{ 'table-active': t.sel.has(id(row)) }"
              @click="isRowClick($event) && d.view(row)" @keydown.enter.self.prevent="d.toggle(row)" @keydown.space.self.prevent="d.toggle(row)">
              <td v-if="cols.on.select" class="select-cell"><input class="form-check-input m-0" type="checkbox" aria-label="Select entry"
                :checked="t.sel.has(id(row))" @change="t.toggle(id(row))"></td>
              <td v-if="cols.on.id" class="text-truncate">{{ id(row) }}</td>
              <td v-if="cols.on.jobid" class="text-truncate">{{ row.jobSnapshot?.primaryKey ?? EMPTY }}</td>
              <td v-if="cols.on.parent" class="text-truncate">
                <a v-if="row.jobSnapshot?.parentId" href="#" class="text-decoration-none" @click.prevent="t.only('parent_id', row.jobSnapshot.parentId)">{{ row.jobSnapshot.parentId }}</a>
                <template v-else>{{ EMPTY }}</template>
              </td>
              <td v-if="cols.on.group" class="text-truncate">{{ row.jobSnapshot?.groupKey || EMPTY }}</td>
              <td v-if="cols.on.kind" class="text-truncate">{{ row.jobSnapshot?.kind || EMPTY }}</td>
              <td v-if="cols.on.payload" class="text-truncate" :title="jsonText(row.jobSnapshot?.payload)">{{ truncatePayload(row.jobSnapshot?.payload) }}</td>
              <td v-if="cols.on.failed" class="text-truncate" :title="formatTime(row.failedAt, EMPTY)">{{ formatAge(row.failedAt) }}</td>
              <td v-if="cols.on.result" class="text-center">
                <span v-if="row.result != null" style="color: var(--arb-teal-on-bg);" title="Has a stored result">&#x2713;</span>
                <span v-else class="text-muted">{{ EMPTY }}</span>
              </td>
              <td v-if="cols.on.inserted" class="text-truncate" :title="formatTime(row.jobSnapshot?.insertedAt, EMPTY)">{{ formatAge(row.jobSnapshot?.insertedAt) }}</td>
              <td v-if="cols.on.completed" class="text-truncate" :title="formatTime(row.completedAt, EMPTY)">{{ formatAge(row.completedAt) }}</td>
              <td v-if="cols.on.attempts">{{ row.jobSnapshot?.attempts ?? EMPTY }}</td>
              <td v-if="cols.on.error" class="text-truncate" :title="row.jobSnapshot?.lastError">{{ truncate(row.jobSnapshot?.lastError) }}</td>
              <td v-if="cols.on.gates"><gate-badges :rate="row.jobSnapshot?.rateLimit" :conc="row.jobSnapshot?.concurrency"/></td>
              <td v-if="cols.on.actions" class="cell-actions">
                <action-menu row :detail="() => d.view(row)" :disabled="busy.has(id(row))" v-slot="{ close }"><snapshot-menu :row="row" :close="close"/></action-menu>
              </td>
            </tr>
          </tbody>
          <skeleton-rows :view="view" :span="cols.shown.length" :empty="!rows.length">{{ cfg.empty }}</skeleton-rows>
        </table>
      </div>
      <pager :t="t" :view="view" bottom/>

      <drawer :d="d" :title="d.cur ? cfg.title + ' ' + id(d.cur) : cfg.title" :sticky="edit.id != null">
        <template #actions>
          <action-menu v-if="d.cur" :disabled="busy.has(id(d.cur))" v-slot="{ close }"><snapshot-menu :row="d.cur" :close="close"/></action-menu>
        </template>
        <div v-if="edit.id != null" class="offcanvas-body drawer-edit arb-edit" tabindex="0">
          <div class="edit-field">
            <label class="form-label edit-label" for="payloadEditText">Payload (JSON)</label>
            <textarea id="payloadEditText" class="form-control font-monospace payload-edit-text" rows="12" spellcheck="false"
              v-model="edit.text" :class="{ 'is-invalid': editInvalid }"
              @keydown.ctrl.enter.prevent="submitEdit" @keydown.meta.enter.prevent="submitEdit"></textarea>
            <small class="edit-hint">The queue reads the payload again. Its kind, rate-limit key and concurrency key come from the new payload.</small>
          </div>
          <edit-actions :form="edit" :disabled="editInvalid || busy.has(edit.id)" error-class="payload-edit-error"
            @cancel="cancelEdit" @save="submitEdit">{{ edit.saving ? 'Sending…' : cfg.sendLabel }}</edit-actions>
        </div>
        <div v-else-if="d.cur" class="offcanvas-body" tabindex="0">
          <dl class="row">
            <kv :l="cfg.idLabel">{{ id(d.cur) }}</kv>
            <kv v-if="d.cur.failedAt" l="Failed At">{{ formatTime(d.cur.failedAt, EMPTY) }}</kv>
            <kv v-if="d.cur.completedAt" l="Inserted At">{{ formatTime(d.cur.jobSnapshot?.insertedAt, EMPTY) }}</kv>
            <kv v-if="d.cur.completedAt" l="Completed At">{{ formatTime(d.cur.completedAt, EMPTY) }}</kv>
            <kv l="Original Job ID">{{ d.cur.jobSnapshot?.primaryKey }}</kv>
            <kv l="Parent ID">{{ d.cur.jobSnapshot?.parentId || EMPTY }}</kv>
            <kv l="Group Key">{{ d.cur.jobSnapshot?.groupKey || EMPTY }}</kv>
            <kv l="Attempts">{{ d.cur.jobSnapshot?.attempts }}</kv>
            <kv v-if="cfg.tab === 'dlq'" l="Last Error" wide>
              <copy-block v-if="d.cur.jobSnapshot?.lastError" :text="d.cur.jobSnapshot.lastError"/>
              <template v-else>{{ EMPTY }}</template>
            </kv>
            <kv l="Payload" wide><copy-block :text="formatJson(d.cur.jobSnapshot?.payload)"/></kv>
            <kv v-if="d.cur.jobSnapshot?.parentState != null" l="Child Results" wide><copy-block :text="formatJson(d.cur.jobSnapshot.parentState)"/></kv>
            <kv v-if="d.cur.result != null" l="Result" wide><copy-block :text="formatJson(d.cur.result)"/></kv>
          </dl>
        </div>
      </drawer>`,
  };
}

export const DlqTab = snapshotTab(DLQ);
export const ArchiveTab = snapshotTab(ARCHIVE);
