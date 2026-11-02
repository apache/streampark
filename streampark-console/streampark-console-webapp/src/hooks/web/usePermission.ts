import { useUserStore } from '/@/store/modules/user';
import { RoleEnum } from '/@/enums/roleEnum';

import { isArray } from '/@/utils/is';

const ADMIN_ONLY_ACTIONS = new Set([
  'cluster:create',
  'cluster:update',
  'externalLink:create',
  'externalLink:delete',
  'externalLink:update',
  'setting:update',
  'setting:view',
  'token:add',
  'token:delete',
  'token:view',
  'user:add',
  'user:delete',
  'user:reset',
  'user:types',
  'user:update',
  'yarnQueue:create',
  'yarnQueue:delete',
  'yarnQueue:update',
]);

// Fixed role checks for actions that are visible in the UI.
export function usePermission() {
  const userStore = useUserStore();

  /**
   * Determine whether there is permission
   */
  function hasPermission(value?: RoleEnum | RoleEnum[] | string | string[], def = true): boolean {
    // Visible by default
    if (!value || userStore.getRoleList?.includes(RoleEnum.ADMIN)) {
      return def;
    }

    const actions = isArray(value) ? value : [value];
    if (userStore.getRoleList?.includes(RoleEnum.EDITOR)) {
      return actions.some((action) => !ADMIN_ONLY_ACTIONS.has(action as string));
    }

    return false;
  }

  return { hasPermission };
}
