// Shared components and the DOM behavior they need. Bootstrap supplies CSS only.
import { ref, computed, watch, nextTick, onBeforeUnmount, onDeactivated } from '../vendor/vue.esm-browser.prod.js';
import { TIMING, DRAWER_MODAL_MQ } from './config.js';
import { PERCENT } from './format.js';
import { app, toast, dismissToast, holdToast, load, save } from './store.js';
import { useTick } from './use.js';

// True when a row click should open its detail, not hit a control the row owns.
export function isRowClick(e) {
  return !(e.target instanceof Element && e.target.closest('a, button, input, select, label, .dropdown, [role=button]'));
}

const inField = (e) => e.target instanceof Element && e.target.closest('input, textarea, select, [contenteditable]');

// Adds document listeners until the returned function is called.
function listen(pairs) {
  for (const [type, fn, capture] of pairs) document.addEventListener(type, fn, capture);
  return () => {
    for (const [type, fn, capture] of pairs) document.removeEventListener(type, fn, capture);
  };
}

const FOCUSABLE = 'a[href], button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])';

const menuItems = (container) => [...container.querySelectorAll(FOCUSABLE)].filter((el) => !el.closest('.disabled'));

// Arrow keys walk a menu's items. From outside the menu, ArrowUp starts at the last item.
function moveFocus(container, e) {
  const items = menuItems(container);
  if (!items.length) return;
  e.preventDefault();
  const i = items.indexOf(document.activeElement);
  const next = i < 0 ? (e.key === 'ArrowDown' ? 0 : items.length - 1) : i + (e.key === 'ArrowDown' ? 1 : -1);
  items[(next + items.length) % items.length].focus();
}

// Tab and Shift+Tab wrap inside container.
function trapFocus(container, e) {
  const items = [...container.querySelectorAll(FOCUSABLE)].filter((el) => el.offsetParent);
  if (!items.length) return;
  const edge = e.shiftKey ? items[0] : items.at(-1);
  if (document.activeElement !== edge && container.contains(document.activeElement) && document.activeElement !== container) return;
  e.preventDefault();
  (e.shiftKey ? items.at(-1) : items[0]).focus();
}

// Body scroll stays locked while any modal surface holds a lock.
let scrollLocks = 0;

// Padding takes the place of the hidden scrollbar, so the page does not shift.
function scrollLock() {
  let held = true;
  if (scrollLocks++ === 0) {
    const gap = innerWidth - document.documentElement.clientWidth;
    Object.assign(document.body.style, { overflow: 'hidden', paddingRight: gap > 0 ? gap + 'px' : '' });
  }
  return () => {
    if (!held) return;
    held = false;
    if (--scrollLocks === 0) Object.assign(document.body.style, { overflow: '', paddingRight: '' });
  };
}

async function copyText(text, btn) {
  const value = text == null ? '' : String(text);
  try {
    if (navigator.clipboard && window.isSecureContext) await navigator.clipboard.writeText(value);
    else copyBySelection(value);
    btn.classList.add('copied');
    setTimeout(() => btn.classList.remove('copied'), TIMING.copiedFlashMs);
  } catch (e) {
    toast('Copy failed: ' + e.message);
  }
}

// The Clipboard API exists only in a secure context.
function copyBySelection(value) {
  const ta = document.createElement('textarea');
  ta.value = value;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  const ok = document.execCommand('copy');
  ta.remove();
  if (!ok) throw new Error('the browser refused');
}

// Renders its slot against the clock, so a tick re-renders the slot and not the table.
export const Tick = {
  setup(_, { slots }) {
    const now = useTick();
    return () => slots.default?.({ now: now.value });
  },
};

// ---- Dropdown ----

const MENU_GAP_PX = 2;
// Above the drawer and modals, since the menu lives on the body.
const MENU_Z = 1070;

// A menu placed with fixed coordinates, so a table that scrolls cannot clip it.
// stay keeps it open for clicks inside, for items that arm on the first click.
export const DropDown = {
  props: {
    label: [String, Number],
    toggleClass: { type: String, default: 'btn btn-outline-secondary btn-sm dropdown-toggle' },
    end: Boolean,
    stay: Boolean,
    disabled: Boolean,
    menuClass: String,
    menuStyle: Object,
    title: String,
    ariaLabel: String,
  },
  setup(props) {
    const open = ref(false);
    const btn = ref(/** @type {HTMLElement | null} */ (null));
    const menu = ref(/** @type {HTMLElement | null} */ (null));
    const pos = ref({});
    let unlisten = null;
    let frame = 0;
    // An item that arms changes the menu's width, so a resize places the menu again.
    let resized = null;

    function place() {
      if (!btn.value || !menu.value) return;
      const r = btn.value.getBoundingClientRect();
      const h = menu.value.scrollHeight;
      const w = menu.value.offsetWidth;
      const roomBelow = innerHeight - r.bottom - MENU_GAP_PX;
      const roomAbove = r.top - MENU_GAP_PX;
      // A menu that fits on neither side opens on the roomier one and scrolls.
      const below = h <= roomBelow || roomBelow >= roomAbove;
      const room = below ? roomBelow : roomAbove;
      const fitsEnd = r.right - w >= 0;
      const fitsStart = r.left + w <= innerWidth;
      const left = (props.end ? fitsEnd || !fitsStart : !fitsStart && fitsEnd) ? r.right - w : r.left;
      pos.value = {
        top: (below ? r.bottom + MENU_GAP_PX : r.top - Math.min(h, room) - MENU_GAP_PX) + 'px',
        left: left + 'px',
        maxHeight: room + 'px',
        overflowY: 'auto',
      };
    }

    // A scroll places the menu at most once per frame.
    const placeSoon = () => {
      frame ||= requestAnimationFrame(() => {
        frame = 0;
        place();
      });
    };

    // A focused item leaves with the menu, so focus returns to the toggle.
    function close(refocus) {
      if (!open.value) return;
      const inMenu = !!menu.value?.contains(document.activeElement);
      open.value = false;
      cancelAnimationFrame(frame);
      frame = 0;
      unlisten?.();
      unlisten = null;
      resized?.disconnect();
      resized = null;
      removeEventListener('resize', place);
      if (refocus === true || inMenu) btn.value?.focus();
    }

    // Tab steps through the menu as if it sat right after the toggle.
    function onTab(e) {
      const items = menuItems(menu.value);
      const at = document.activeElement;
      if (at === btn.value && !e.shiftKey && items.length) {
        e.preventDefault();
        items[0].focus();
        return;
      }
      const i = items.indexOf(at);
      const next = i + (e.shiftKey ? -1 : 1);
      if (i >= 0 && next >= 0 && next < items.length) {
        e.preventDefault();
        items[next].focus();
        return;
      }
      if (i >= 0 && e.shiftKey) e.preventDefault();
      close(i >= 0);
    }

    async function toggle() {
      if (open.value) return close();
      open.value = true;
      await nextTick();
      if (!open.value) return;
      place();
      unlisten = listen([
        [
          'click',
          (e) => {
            if (!btn.value?.contains(e.target) && !menu.value?.contains(e.target)) close();
          },
        ],
        // Capture, so an open menu takes Escape before the drawer under it.
        [
          'keydown',
          (e) => {
            if (e.key === 'Escape') {
              e.preventDefault();
              close(true);
            } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') moveFocus(menu.value, e);
            else if (e.key === 'Tab') onTab(e);
          },
          true,
        ],
        ['scroll', placeSoon, true],
      ]);
      addEventListener('resize', place);
      if (menu.value) {
        resized = new ResizeObserver(placeSoon);
        resized.observe(menu.value);
      }
    }

    // ArrowDown or ArrowUp on a closed toggle opens the menu at its first or last item.
    async function onToggleKey(e) {
      if (open.value || (e.key !== 'ArrowDown' && e.key !== 'ArrowUp')) return;
      e.preventDefault();
      await toggle();
      if (menu.value) moveFocus(menu.value, e);
    }

    const onMenuClick = (e) => {
      if (!props.stay && e.target.closest('.dropdown-item')) close();
    };
    onDeactivated(close);
    onBeforeUnmount(close);
    return { open, btn, menu, pos, toggle, close, onToggleKey, onMenuClick, MENU_Z };
  },
  template: /* html */ `
    <button ref="btn" type="button" :class="[toggleClass, { show: open }]" :disabled="disabled" :title="title" :aria-label="ariaLabel"
      :aria-expanded="open" @click="toggle" @keydown="onToggleKey"><slot name="label">{{ label + ' ' }}</slot></button>
    <teleport to="body">
      <div v-if="open" ref="menu" class="dropdown-menu show" :class="menuClass"
        :style="{ position: 'fixed', margin: 0, zIndex: MENU_Z, ...menuStyle, ...pos }" @click="onMenuClick">
        <slot :close="close"></slot>
      </div>
    </teleport>`,
};

// ---- Confirm-on-second-click button ----

export const ArmButton = {
  props: {
    arm: Object,
    k: String,
    label: String,
    confirm: String,
    cls: { type: String, default: 'btn btn-sm' },
    on: String,
    off: String,
    disabled: Boolean,
  },
  emits: ['fire'],
  template: /* html */ `
    <button type="button" :class="[cls, arm.is(k) ? on : off]" :disabled="disabled"
      @click="arm.fire(k) && $emit('fire')"><slot></slot>{{ arm.is(k) ? confirm : label }}</button>`,
};

// ---- Modal ----

// A dialog on a backdrop. Escape, the backdrop and Cancel close it.
export const Modal = {
  props: { open: Boolean, title: String, subject: String, size: String, error: String },
  emits: ['update:open'],
  setup(props, { emit }) {
    const shown = ref(false);
    const root = ref(/** @type {HTMLElement | null} */ (null));
    let unlock = null;
    let unlisten = null;
    // The element focused before the open, to take focus back at the close.
    let back = /** @type {HTMLElement | null} */ (null);
    const close = () => emit('update:open', false);
    // The topmost modal takes the keys, also when focus has left it.
    function onKey(e) {
      if (e.defaultPrevented || !root.value || root.value !== [...document.querySelectorAll('.modal')].at(-1)) return;
      if (e.key === 'Tab') return trapFocus(root.value, e);
      if (e.key !== 'Escape') return;
      e.preventDefault();
      close();
    }
    // A press that ends off the backdrop, such as on its scrollbar, is not a dismiss.
    let pressed = false;
    const press = (e) => {
      pressed = e.target === e.currentTarget;
    };
    const release = (e) => {
      if (pressed && e.target === e.currentTarget) close();
      pressed = false;
    };
    watch(
      () => props.open,
      async (open) => {
        unlock?.();
        unlock = open ? scrollLock() : null;
        unlisten?.();
        unlisten = open ? listen([['keydown', onKey]]) : null;
        if (!open) {
          shown.value = false;
          await nextTick();
          if (document.activeElement === document.body && back?.isConnected) back.focus();
          back = null;
          return;
        }
        back = /** @type {HTMLElement | null} */ (document.activeElement);
        await nextTick();
        requestAnimationFrame(() => {
          shown.value = props.open;
        });
        /** @type {HTMLElement | null | undefined} */ (root.value?.querySelector('input, textarea, select') || root.value)?.focus();
      },
      { immediate: true },
    );
    onDeactivated(close);
    onBeforeUnmount(() => {
      unlock?.();
      unlisten?.();
    });
    return { shown, root, close, press, release };
  },
  template: /* html */ `
    <teleport to="body">
      <template v-if="open">
        <div ref="root" class="modal fade d-block" :class="{ show: shown }" tabindex="-1" role="dialog" aria-modal="true"
          :aria-label="title" @mousedown="press" @click="release">
          <div class="modal-dialog" :class="size && 'modal-' + size">
            <div class="modal-content">
              <div class="modal-header">
                <div class="modal-heading">
                  <h2 class="modal-title">{{ title }}</h2>
                  <p class="modal-subject" :title="subject">{{ subject }}</p>
                </div>
                <button type="button" class="btn-close" aria-label="Close" @click="close"></button>
              </div>
              <div class="modal-body"><slot></slot></div>
              <div class="modal-footer">
                <small class="alert alert-danger mb-0 inline-error me-auto" v-if="error">{{ error }}</small>
                <button type="button" class="btn btn-secondary btn-sm" @click="close">Cancel</button>
                <slot name="footer"></slot>
              </div>
            </div>
          </div>
        </div>
        <div class="modal-backdrop fade" :class="{ show: shown }"></div>
      </template>
    </teleport>`,
};

// Type-to-confirm: the action unlocks once the reader types target.
export const ConfirmModal = {
  props: { open: Boolean, title: String, subject: String, note: String, prompt: String, target: String, action: String, busy: Boolean },
  emits: ['update:open', 'confirm'],
  setup(props, { emit }) {
    const text = ref('');
    watch(
      () => props.open,
      () => {
        text.value = '';
      },
    );
    const valid = computed(() => props.target !== '' && text.value === props.target);
    const confirm = () => {
      if (!valid.value || props.busy) return;
      emit('update:open', false);
      emit('confirm');
    };
    return { text, valid, confirm };
  },
  template: /* html */ `
    <modal :open="open" @update:open="$emit('update:open', $event)" :title="title" :subject="subject">
      <p class="text-muted small mb-3">{{ note }}</p>
      <label class="form-label small">{{ prompt }} <code v-if="target !== subject">{{ target }}</code></label>
      <input type="text" class="form-control form-control-sm" v-model="text" autocomplete="off" spellcheck="false"
        :placeholder="target" @keydown.enter.prevent="confirm">
      <template #footer>
        <button type="button" class="btn btn-warning btn-sm" :disabled="!valid || busy" @click="confirm">{{ action }}</button>
      </template>
    </modal>`,
};

// ---- Drawer ----

const DRAWER_MIN_PX = 320;
const DRAWER_MAX_FRACTION = 0.9;
const DRAWER_STEP_PX = 16;
const DRAWER_BIG_STEP_PX = 64;
const DRAWER_WIDTH_KEY = 'arb.drawerWidth';

function setDrawerWidth(px) {
  const w = Math.round(Math.max(DRAWER_MIN_PX, Math.min(px, innerWidth * DRAWER_MAX_FRACTION)));
  document.documentElement.style.setProperty('--arb-drawer-w', w + 'px');
  return w;
}

const saveDrawerWidth = (w) => save(DRAWER_WIDTH_KEY, w);

const storedDrawerWidth = load(DRAWER_WIDTH_KEY, null);
if (Number.isFinite(storedDrawerWidth)) setDrawerWidth(storedDrawerWidth);

// The detail panel for a useDetail. Wide, it stays beside the list, so the list
// stays readable and clickable. Narrow, it covers the list and holds the focus
// and the scroll. sticky holds it open against outside clicks.
export const Drawer = {
  props: { d: Object, title: String, status: String, statusClass: String, sticky: Boolean },
  setup(props) {
    const el = ref(/** @type {HTMLElement | null} */ (null));
    const state = ref('');
    const modal = ref(false);
    const modalQuery = matchMedia(DRAWER_MODAL_MQ);
    let back = null;
    let hideTimer = null;
    let unlisten = null;
    let unlock = null;

    const onClick = (e) => {
      const t = e.target;
      if (!(t instanceof Element) || !t.isConnected || el.value?.contains(t) || props.sticky) return;
      if (t.closest('.detail-row, .dropdown-menu, .modal, .toast')) return;
      props.d.close();
    };
    const onKey = (e) => {
      if (e.defaultPrevented) return;
      if (e.key === 'Escape') {
        if (!inField(e) && !document.querySelector('.modal') && !props.d.held()) props.d.close();
        return;
      }
      if (e.key === 'Tab') {
        if (modal.value && el.value && !document.querySelector('.modal')) trapFocus(el.value, e);
        return;
      }
      if (e.metaKey || e.ctrlKey || e.altKey || inField(e) || (e.key !== 'j' && e.key !== 'k')) return;
      // A modal or an open menu over the drawer owns the keyboard.
      if (document.querySelector('.modal, .dropdown-menu.show')) return;
      e.preventDefault();
      props.d.step(e.key === 'k' ? -1 : 1);
    };

    function setModal(on) {
      modal.value = on;
      if (on) unlock ??= scrollLock();
      else {
        unlock?.();
        unlock = null;
      }
    }
    const onMedia = (e) => {
      if (state.value === 'show') setModal(e.matches);
    };

    function show() {
      clearTimeout(hideTimer);
      if (state.value !== 'show') back = document.activeElement;
      setModal(modalQuery.matches);
      state.value = 'show';
      unlisten ??= listen([
        ['click', onClick],
        ['keydown', onKey],
      ]);
      nextTick(() => {
        if (!el.value?.contains(document.activeElement)) /** @type {HTMLElement | null | undefined} */ (el.value?.querySelector('.drawer-close'))?.focus();
      });
    }

    // Focus returns only when the close strands it. The reader may have clicked elsewhere.
    function hide() {
      unlisten?.();
      unlisten = null;
      unlock?.();
      unlock = null;
      if (!state.value) return;
      state.value = 'hiding';
      hideTimer = setTimeout(() => {
        state.value = '';
      }, TIMING.drawerSlideMs);
      const stranded = document.activeElement === document.body || el.value?.contains(document.activeElement);
      if (stranded && back?.isConnected) back.focus();
      back = null;
    }

    function resizeStart(e) {
      e.preventDefault();
      document.body.classList.add('drawer-resizing');
      let w = null;
      const end = () => {
        stop();
        document.body.classList.remove('drawer-resizing');
        if (w != null) saveDrawerWidth(w);
      };
      const stop = listen([
        [
          'pointermove',
          (ev) => {
            w = setDrawerWidth(innerWidth - ev.clientX);
          },
        ],
        ['pointerup', end],
        ['pointercancel', end],
      ]);
    }

    function resizeKey(e) {
      if (!el.value || (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight')) return;
      e.preventDefault();
      const step = e.shiftKey ? DRAWER_BIG_STEP_PX : DRAWER_STEP_PX;
      saveDrawerWidth(setDrawerWidth(el.value.offsetWidth + (e.key === 'ArrowLeft' ? step : -step)));
    }

    watch(
      () => props.d.open,
      (open) => (open ? show() : hide()),
      { immediate: true },
    );
    modalQuery.addEventListener('change', onMedia);
    onBeforeUnmount(() => {
      modalQuery.removeEventListener('change', onMedia);
      hide();
      clearTimeout(hideTimer);
    });
    return { el, state, modal, resizeStart, resizeKey };
  },
  template: /* html */ `
    <div ref="el" class="offcanvas offcanvas-end detail-drawer" :class="{ show: state, hiding: state === 'hiding' }"
      tabindex="-1" role="dialog" :aria-modal="modal" :aria-label="title">
      <div class="drawer-resize" role="separator" aria-orientation="vertical" aria-label="Resize panel" tabindex="0"
        @pointerdown="resizeStart" @keydown="resizeKey"></div>
      <div class="offcanvas-header">
        <div class="drawer-head">
          <div class="drawer-head-text">
            <span class="drawer-title">{{ title }}</span>
            <span class="drawer-sub">
              <span v-if="status" class="badge" :class="statusClass">{{ status }}</span>
              <span v-if="d.position">{{ d.position }}</span>
            </span>
          </div>
          <div class="drawer-actions">
            <slot name="actions"></slot>
            <div class="drawer-nav">
              <button type="button" class="drawer-nav-btn" :disabled="!d.neighbour(-1)" @click="d.step(-1)" title="Previous (k)" aria-label="Previous">&#8593;</button>
              <button type="button" class="drawer-nav-btn" :disabled="!d.neighbour(1)" @click="d.step(1)" title="Next (j)" aria-label="Next">&#8595;</button>
            </div>
            <button type="button" class="drawer-close" aria-label="Close" @click="d.close()">&#10005;</button>
          </div>
        </div>
      </div>
      <slot></slot>
    </div>`,
};

// The drawer's Actions menu, or a row's ⋮ menu. detail opens the row's drawer.
export const ActionMenu = {
  props: { disabled: Boolean, row: Boolean, detail: Function },
  template: /* html */ `
    <div class="dropdown">
      <drop-down :label="row ? '\u22ee' : 'Actions'" :toggle-class="row ? 'btn btn-row-actions btn-sm' : 'drawer-actions-btn dropdown-toggle'"
        :title="row ? 'Row actions' : undefined" :aria-label="row ? 'Row actions' : undefined" end stay :disabled="disabled">
        <template #default="{ close }">
          <button v-if="detail" type="button" class="dropdown-item" @click="detail(); close()">Detail</button>
          <slot :close="close"></slot>
        </template>
      </drop-down>
    </div>`,
};

// ---- Tables ----

export const SortTh = {
  props: { sort: Object, k: String },
  computed: {
    caret() {
      if (this.sort.by !== this.k) return '↕';
      return this.sort.dir === 'asc' ? '▲' : '▼';
    },
    ariaSort() {
      if (this.sort.by !== this.k) return 'none';
      return this.sort.dir === 'asc' ? 'ascending' : 'descending';
    },
  },
  template: /* html */ `
    <th class="sortable" tabindex="0" :aria-sort="ariaSort" @click="sort.toggle(k)"
      @keydown.enter.prevent="sort.toggle(k)" @keydown.space.prevent="sort.toggle(k)"><slot></slot><span class="sort-caret" aria-hidden="true">{{ caret }}</span></th>`,
};

// A header row over column defs: { key, label, weight?, sort?, title?, cls? }.
// Weights split a fixed-layout row. A 'select' column holds the select-all box.
export const THead = {
  props: { cols: Array, sort: Object, table: Object },
  setup(props) {
    const total = computed(() => props.cols.reduce((n, c) => n + (c.weight || 0), 0));
    const width = (c) => (c.weight ? { width: (c.weight / total.value) * PERCENT + '%' } : {});
    return { width };
  },
  template: /* html */ `
    <thead><tr>
      <template v-for="c in cols" :key="c.key">
        <th v-if="c.key === 'select'" class="select-cell" :style="width(c)">
          <input class="form-check-input m-0" type="checkbox" title="Select all on this page" aria-label="Select all on this page"
            :checked="table.allSelected" @change="table.toggleAll(); $event.target.checked = table.allSelected">
        </th>
        <sort-th v-else-if="c.sort" :sort="sort" :k="c.sort" :class="c.cls" :style="width(c)" :title="c.title"><slot :name="c.key">{{ c.label }}</slot></sort-th>
        <th v-else :class="c.cls" :style="width(c)" :title="c.title"><slot :name="c.key">{{ c.label }}</slot></th>
      </template>
    </tr></thead>`,
};

const SKELETON_ROWS = 5;

// Placeholder rows for a first load slow enough to earn them, else the slot as the empty row.
export const SkeletonRows = {
  props: { view: Object, span: Number, empty: Boolean },
  setup: () => ({ SKELETON_ROWS }),
  template: /* html */ `
    <tbody v-if="view.slow && !view.loaded">
      <tr v-for="i in SKELETON_ROWS" :key="i" class="skeleton-row"><td :colspan="span"><span class="skeleton-bar"></span></td></tr>
    </tbody>
    <tbody v-else-if="empty && view.loaded && !view.errored">
      <tr><td :colspan="span" class="text-muted text-center"><slot></slot></td></tr>
    </tbody>`,
};

// The panel a view shows in place of its rows when its first load failed.
export const LoadError = {
  props: { view: Object, t: Object },
  template: /* html */ `
    <div class="empty-state" role="alert" v-if="view.failed">
      <svg class="empty-state-icon is-error" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.3">
        <path d="M12 4.4 21.2 19.4H2.8z" stroke-linejoin="round"/>
        <path d="M12 10.2v3.6M12 16.6h.01" stroke-linecap="round"/>
      </svg>
      <p class="empty-state-title">Could not load {{ view.noun }}</p>
      <p class="empty-state-note">{{ view.error }}</p>
      <button type="button" class="btn btn-outline-secondary btn-sm empty-state-action" :disabled="view.loading" @click="view.reload()"
        ><span :class="view.loading ? 'spin' : 'd-none'" aria-hidden="true">&#x21bb;</span> {{ view.loading ? 'Trying…' : 'Try again' }}</button>
      <button v-if="t?.filtered" type="button" class="btn btn-outline-secondary btn-sm empty-state-action" :disabled="view.loading" @click="t.clear()">Clear filters</button>
    </div>`,
};

// Refresh now, plus the auto-refresh interval.
export const RefreshControl = {
  props: { view: Object },
  setup() {
    return { modes: Object.keys(TIMING.refreshModes) };
  },
  template: /* html */ `
    <div class="btn-group btn-group-sm">
      <button class="btn btn-outline-secondary" :disabled="view.spinning" @click="view.reload()" title="Refresh now" aria-label="Refresh now"><span :class="{ spin: view.spinning }">&#x21bb;</span></button>
      <drop-down toggle-class="btn btn-outline-secondary dropdown-toggle refresh-interval" title="Auto-refresh interval" end :label="view.mode === 'paused' ? 'Off' : view.mode">
        <button v-for="m in modes" :key="m" type="button" class="dropdown-item" :class="{ active: view.mode === m }" @click="view.setMode(m)">Every {{ m }}</button>
        <hr class="dropdown-divider">
        <button type="button" class="dropdown-item" :class="{ active: view.mode === 'paused' }" @click="view.setMode('paused')">Off</button>
      </drop-down>
    </div>`,
};

// The count of changes a paused view has not shown yet.
export const PendingNote = {
  props: { view: Object },
  template: /* html */ `<div class="text-info small mb-2" v-if="view.ready && view.mode === 'paused' && view.pending > 0">{{ view.pending }} {{ pluralize(view.pending, 'change') }} pending</div>`,
};

// The pager for a useTable. The top one carries the count, page jump and page size.
export const Pager = {
  props: { t: Object, view: Object, one: String, many: String, bottom: Boolean },
  setup() {
    return { sizes: TIMING.pageSizes };
  },
  template: /* html */ `
    <div v-if="view.ready && (!bottom || t.pages > 1)" class="d-flex align-items-center gap-2" :class="bottom ? 'mb-4' : ['mb-2', { invisible: !view.loaded }]">
      <div class="btn-group btn-group-sm" v-show="t.pages > 1">
        <button class="btn btn-outline-secondary" :disabled="t.page === 1" @click="t.goTo(t.page - 1)">Prev</button>
        <button class="btn btn-outline-secondary" :disabled="t.page >= t.pages" @click="t.goTo(t.page + 1)">Next</button>
      </div>
      <span v-if="bottom" class="text-muted small">Page {{ t.page }} of {{ t.pages }}</span>
      <template v-else>
        <span class="text-muted small">{{ t.total }} {{ pluralize(t.total, one, many) }}<template v-if="t.pages > 1"> · page {{ t.page }} of {{ t.pages }}</template></span>
        <input type="number" class="form-control form-control-sm page-jump" min="1" :max="t.pages" v-show="t.pages > 1"
          :placeholder="t.page" @keyup.enter="t.goTo($event.target.value); $event.target.value = ''" title="Jump to page" aria-label="Jump to page">
        <div class="btn-group btn-group-sm ms-auto">
          <drop-down end :label="t.limit + ' / page'" title="Rows per page" toggle-class="btn btn-outline-secondary dropdown-toggle">
            <button v-for="n in sizes" :key="n" type="button" class="dropdown-item" :class="{ active: n === t.limit }" @click="t.setLimit(n)">{{ n }} per page</button>
          </drop-down>
        </div>
      </template>
    </div>`,
};

// Filter chips plus a field-and-value adder, over a useTable.
export const FilterBuilder = {
  props: { t: Object },
  template: /* html */ `
    <div class="filter-builder">
      <span v-for="c in t.chips" :key="c.param" class="filter-chip">
        <span class="filter-chip-label">{{ c.label }}:</span>
        <span class="filter-chip-value">{{ c.value }}</span>
        <button type="button" class="filter-chip-x" @click="t.set(c.param, '')" :aria-label="'Remove ' + c.label + ' filter'">&#x2715;</button>
      </span>
      <div class="input-group input-group-sm" style="width: auto;">
        <drop-down :label="t.field.label" title="Filter field" toggle-class="btn btn-outline-secondary dropdown-toggle">
          <button v-for="f in t.fields" :key="f.param" type="button" class="dropdown-item" :class="{ active: t.draft.param === f.param }"
            @click="t.draft.param = f.param; t.draft.value = ''">{{ f.label }}</button>
        </drop-down>
        <select v-if="t.field.options && t.kinds.length" class="form-select filter-value" aria-label="Filter value" v-model="t.draft.value">
          <option value="">{{ t.field.label }}…</option>
          <option v-for="o in t.kinds" :key="o" :value="o">{{ o }}</option>
        </select>
        <input v-else :type="t.field.time ? 'datetime-local' : 'text'" class="form-control filter-value" :placeholder="t.field.label + '…'"
          aria-label="Filter value" v-model="t.draft.value" @keyup.enter="t.add()" @keyup.escape="t.draft.value = ''">
        <button class="btn btn-outline-secondary" type="button" @click="t.add()" :disabled="!t.draft.value" title="Add filter" aria-label="Add filter">+</button>
      </div>
    </div>`,
};

// Column show/hide over a useColumns.
export const ColumnsMenu = {
  props: { cols: Object },
  template: /* html */ `
    <div class="dropdown">
      <drop-down label="Columns" title="Show/hide columns" stay menu-class="p-2" :menu-style="{ minWidth: '12rem' }">
        <label v-for="c in cols.menu" :key="c.key" class="dropdown-item d-flex align-items-center gap-2 mb-0">
          <input type="checkbox" class="form-check-input mt-0" :checked="cols.on[c.key]" @change="cols.toggle(c.key)">
          <span>{{ c.label }}</span>
        </label>
        <hr class="dropdown-divider">
        <button type="button" class="dropdown-item" @click="cols.reset()">Reset to defaults</button>
      </drop-down>
    </div>`,
};

// The selection count and its bulk actions.
export const BulkBar = {
  props: { t: Object },
  template: /* html */ `
    <div class="align-items-center gap-2 mb-2" :class="t.sel.size ? 'd-flex' : 'd-none'">
      <span class="small fw-semibold">{{ t.sel.size }} selected</span>
      <slot></slot>
      <button class="btn btn-link btn-sm text-muted text-decoration-none p-0 ms-1" @click="t.sel.clear()">Clear</button>
    </div>`,
};

// ---- Small pieces ----

// A copy button over a payload block.
export const CopyBlock = {
  props: { text: [String, Number] },
  setup() {
    return { copyText };
  },
  template: /* html */ `
    <div class="copy-wrap">
      <button type="button" class="copy-btn" title="Copy" aria-label="Copy to clipboard" @click="copyText(text, $event.currentTarget)"></button>
      <pre class="payload-display p-2 rounded">{{ text ?? '' }}</pre>
    </div>`,
};

// A job's rate-limit and concurrency gates, each a link to its policy.
export const GateBadges = {
  props: { rate: Object, conc: Object },
  template: /* html */ `
    <span v-if="rate || conc" class="gate-badges">
      <a v-for="[g, kind, view, cls] in [[rate, 'Rate limit', 'ratelimits', 'is-rate'], [conc, 'Concurrency', 'concurrency', 'is-conc']]" :key="cls"
        class="gate-badge" :class="[cls, { 'is-empty': !g }]" v-bind="g ? policyLink(view, g.prefix) : {}" :title="g ? kind + ' · ' + gateLabel(g) + ' — open policy' : ''">{{ g ? g.prefix : '' }}</a>
    </span>
    <span v-else>{{ EMPTY }}</span>`,
};

const FILL_BAR = { thinHeight: '14px', height: '16px', wideLabel: '3.5rem', label: '2.75rem' };

// A thin progress bar with its value beside it.
export const FillBar = {
  props: { pct: Number, cls: String, label: [String, Number], thin: Boolean, wide: Boolean },
  setup: () => ({ FILL_BAR }),
  template: /* html */ `
    <div class="d-flex align-items-center gap-2">
      <div class="progress flex-grow-1" :style="{ height: thin ? FILL_BAR.thinHeight : FILL_BAR.height }">
        <div class="progress-bar" :class="cls" :style="{ width: pct + '%' }"></div>
      </div>
      <span class="small text-muted text-end" :style="{ width: wide ? FILL_BAR.wideLabel : FILL_BAR.label }">{{ label ?? pct + '%' }}</span>
    </div>`,
};

// An editor's error and its Cancel and Save buttons. The slot labels Save.
export const EditActions = {
  props: { form: Object, disabled: Boolean, errorClass: String },
  emits: ['cancel', 'save'],
  template: /* html */ `
    <div class="alert alert-danger py-2 mt-3" :class="errorClass" role="alert" v-if="form.error">{{ form.error }}</div>
    <div class="edit-actions">
      <button type="button" class="btn btn-outline-secondary btn-sm" @click="$emit('cancel')">Cancel</button>
      <button type="button" class="btn btn-primary btn-sm" @click="$emit('save')" :disabled="form.saving || disabled"><slot>Save</slot></button>
    </div>`,
};

// A label and its value in a drawer's definition list.
export const Kv = {
  props: { l: String, wide: Boolean },
  template: /* html */ `<dt :class="wide ? 'col-12' : 'col-sm-5'">{{ l }}</dt><dd :class="wide ? 'col-12' : 'col-sm-7'"><slot></slot></dd>`,
};

// One value in a roll-up strip.
export const Qs = {
  props: { v: [String, Number], l: String, c: [String, Object] },
  template: /* html */ `<div class="qs-item"><span class="qs-val" :class="c">{{ v }}</span><span class="qs-lbl">{{ l }}</span></div>`,
};

export const Toasts = {
  setup() {
    const bg = (type) =>
      ({
        success: 'bg-success-subtle text-success-emphasis',
        warning: 'bg-warning-subtle text-warning-emphasis',
        info: 'bg-info-subtle text-info-emphasis',
      })[type] || 'bg-danger-subtle text-danger-emphasis';
    return { app, bg, dismissToast, holdToast };
  },
  template: /* html */ `
    <div class="toast-container position-fixed bottom-0 end-0 p-3" aria-live="polite" aria-atomic="false">
      <div v-for="t in app.toasts" :key="t.id" class="toast show" :class="bg(t.type)" :role="t.type === 'danger' ? 'alert' : 'status'"
        @mouseenter="holdToast(t, true)" @mouseleave="holdToast(t, false)"
        @focusin="holdToast(t, true)" @focusout="!$event.currentTarget.contains($event.relatedTarget) && holdToast(t, false)">
        <div class="d-flex">
          <div class="toast-body">{{ t.message }}</div>
          <span v-if="t.count > 1" class="toast-count badge bg-secondary-subtle text-secondary-emphasis align-self-center me-2">×{{ t.count }}</span>
          <button type="button" class="btn-close me-2 m-auto" aria-label="Dismiss" @click="dismissToast(t)"></button>
        </div>
      </div>
    </div>`,
};

// ---- Scroll edge fades ----

function markScrollEdges(el) {
  // Above the table breakpoint these overflow visibly, and a fade would hide content.
  const overflow = getComputedStyle(el).overflowX;
  const slack = overflow === 'auto' || overflow === 'scroll' ? el.scrollWidth - el.clientWidth : 0;
  el.classList.toggle('scroll-edge-start', slack > 1 && el.scrollLeft > 1);
  el.classList.toggle('scroll-edge-end', slack > 1 && el.scrollLeft < slack - 1);
}

// The scroller and its content are both watched: a table can grow a column in a still box.
const scrollers = new WeakSet();
const edgeSizes = new ResizeObserver((entries) =>
  entries.forEach((e) => {
    markScrollEdges(scrollers.has(e.target) ? e.target : e.target.parentElement);
  }),
);

// The content observed per scroller, so a swapped child moves the observation.
const edgeContent = new WeakMap();

function watchContent(el) {
  const child = el.firstElementChild;
  const prev = edgeContent.get(el);
  if (child === prev) return;
  if (prev) edgeSizes.unobserve(prev);
  if (child) edgeSizes.observe(child);
  edgeContent.set(el, child);
}

// v-scroll-edges on a horizontal scroller fades whichever side has content to reveal.
export const scrollEdges = {
  mounted(el) {
    scrollers.add(el);
    edgeSizes.observe(el);
    watchContent(el);
  },
  updated: watchContent,
  unmounted(el) {
    edgeSizes.unobserve(el);
    const child = edgeContent.get(el);
    if (child) edgeSizes.unobserve(child);
    edgeContent.delete(el);
    scrollers.delete(el);
  },
};

// v-select-on-mount selects an input's value once v-model has written it.
export const selectOnMount = {
  mounted: (el) => nextTick(() => el.select()),
};

// A scroll marks its scroller at most once per frame.
const edgesDue = new Set();
let edgeFrame = 0;

document.addEventListener(
  'scroll',
  (e) => {
    if (!(e.target instanceof Element && scrollers.has(e.target))) return;
    edgesDue.add(e.target);
    edgeFrame ||= requestAnimationFrame(() => {
      edgeFrame = 0;
      edgesDue.forEach(markScrollEdges);
      edgesDue.clear();
    });
  },
  true,
);
