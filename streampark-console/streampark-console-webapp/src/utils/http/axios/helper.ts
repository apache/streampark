import { useMessage } from '/@/hooks/web/useMessage';
import { isObject, isString } from '/@/utils/is';

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss';

export function joinTimestamp<T extends boolean>(
  join: boolean,
  restful: T,
): T extends true ? string : object;

export function joinTimestamp(join: boolean, restful = false): string | object {
  if (!join) {
    return restful ? '' : {};
  }
  const now = new Date().getTime();
  if (restful) {
    return `?_t=${now}`;
  }
  return { _t: now };
}

/**
 * @description: Format request parameter time
 */
export function formatRequestDate(params: Recordable) {
  if (Object.prototype.toString.call(params) !== '[object Object]') {
    return;
  }

  for (const key in params) {
    const format = params[key]?.format ?? null;
    if (format && typeof format === 'function') {
      params[key] = params[key].format(DATE_TIME_FORMAT);
    }
    if (isString(key)) {
      const value = params[key];
      if (value) {
        try {
          params[key] = isString(value) ? value.trim() : value;
        } catch (error: any) {
          throw new Error(error);
        }
      }
    }
    if (isObject(params[key])) {
      formatRequestDate(params[key]);
    }
  }
}

export function requestErrorHandle(error: any) {
  const { Swal, notification } = useMessage();
  if (error.response) {
    if (error.response.data.code == 501) {
      Swal.fire({
        icon: 'error',
        title: 'Oops...',
        text: error.response.data.message,
        footer: '<a href="https://streampark.apache.org/">View the official documentation?</a>',
      });
    } else if (error.response.data.code == 502) {
      let width = document.documentElement.clientWidth || document.body.clientWidth;
      if (width > 1200) {
        width = 1080;
      }
      width *= 0.96;
      Swal.fire({
        icon: 'error',
        title: 'Oops...',
        width: width,
        html: '<pre class="api-exception">' + error.response.data.message + '</pre>',
        footer:
          '<a href="https://github.com/apache/streampark/issues/new/choose">report issue ?</a>',
        focusConfirm: false,
      });
    } else {
      const errorMessage =
        error.response.data === null
          ? 'System error，Please contact the administrator'
          : error.response.data.message;
      switch (error.response.status) {
        case 404:
          notification.error({
            message: 'Sorry, resource not found',
            duration: 4,
          });
          break;
        case 403:
        default:
          notification.error({
            message: errorMessage,
            duration: 4,
          });
          break;
      }
    }
  }
}
