import { createAsyncComponent } from '/@/utils/factory/createAsyncComponent';
import FullScreen from './FullScreen.vue';

export const UserDropDown = createAsyncComponent(() => import('./user-dropdown/index.vue'), {
  loading: true,
});

export const LayoutBreadcrumb = createAsyncComponent(() => import('./Breadcrumb.vue'));

export const Notify = createAsyncComponent(() => import('./notify/index.vue'));
/* GitHub info */
export const Github = createAsyncComponent(() => import('./Github.vue'));
/* Slogan */
export const Slogan = createAsyncComponent(() => import('./Slogan.vue'));
export const LockScreen = createAsyncComponent(() => import('./LockScreen.vue'));

export const ErrorAction = createAsyncComponent(() => import('./ErrorAction.vue'));

export { FullScreen };

export const UserTeam = createAsyncComponent(() => import('./UserTeam.vue'));
