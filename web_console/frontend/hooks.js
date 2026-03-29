import { useEffect, useMemo, useState } from 'react';
import { APP_CONFIG, getPlatformKeys } from './config.js';

export function createInitialScheduleDraft(platforms) {
  return {
    name: '',
    platform: getPlatformKeys(platforms || {})[0] || 'chatgpt-register-v2',
    quantity: '1',
    concurrency: '1',
    schedule_kind: 'daily',
    time_of_day: '',
    schedule_config: {
      weekdays: ['1'],
      day: '1',
      interval_minutes: '5',
      interval_hours: '1',
      minute: '0',
    },
    use_proxy: false,
    auto_import_cpamc: false,
  };
}

export function buildScheduleCronExpression(draft) {
  if (draft.schedule_kind === 'interval-minutes') {
    return `*/${Number(draft.schedule_config?.interval_minutes || 5)} * * * *`;
  }
  if (draft.schedule_kind === 'interval-hours') {
    return `${Number(draft.schedule_config?.minute || 0)} */${Number(draft.schedule_config?.interval_hours || 1)} * * *`;
  }
  const hour = Number((draft.time_of_day || '00:00').split(':')[0] || 0);
  const minute = Number((draft.time_of_day || '00:00').split(':')[1] || 0);
  if (draft.schedule_kind === 'weekly') {
    const weekdays = (draft.schedule_config?.weekdays || []).join(',') || '*';
    return `${minute} ${hour} * * ${weekdays}`;
  }
  if (draft.schedule_kind === 'monthly') {
    return `${minute} ${hour} ${Number(draft.schedule_config?.day || 1)} * *`;
  }
  return `${minute} ${hour} * * *`;
}

export function describeScheduleDraft(draft, tr, weekdayOptions) {
  if (draft.schedule_kind === 'interval-minutes') {
    return tr('schedule_human_interval_minutes', { value: Number(draft.schedule_config?.interval_minutes || 5) });
  }
  if (draft.schedule_kind === 'interval-hours') {
    return tr('schedule_human_interval_hours', {
      hours: Number(draft.schedule_config?.interval_hours || 1),
      minute: String(Number(draft.schedule_config?.minute || 0)).padStart(2, '0'),
    });
  }
  if (draft.schedule_kind === 'weekly') {
    const labels = (draft.schedule_config?.weekdays || [])
      .map((value) => weekdayOptions.find(([key]) => key === value)?.[1])
      .filter(Boolean)
      .join(', ');
    return tr('schedule_human_weekly', { days: labels || '-', time: draft.time_of_day || '--:--' });
  }
  if (draft.schedule_kind === 'monthly') {
    return tr('schedule_human_monthly', { day: Number(draft.schedule_config?.day || 1), time: draft.time_of_day || '--:--' });
  }
  return tr('schedule_human_daily', { time: draft.time_of_day || '--:--' });
}

export function useScheduleBuilder(platforms, tr, weekdayOptions) {
  const [scheduleDraft, setScheduleDraft] = useState(() => createInitialScheduleDraft(platforms));
  const [scheduleErrors, setScheduleErrors] = useState({});

  const cronExpression = useMemo(() => buildScheduleCronExpression(scheduleDraft), [scheduleDraft]);
  const humanSummary = useMemo(() => describeScheduleDraft(scheduleDraft, tr, weekdayOptions), [scheduleDraft, tr, weekdayOptions]);

  useEffect(() => {
    setScheduleErrors(validateScheduleDraft(scheduleDraft, tr));
  }, [scheduleDraft, tr]);

  function resetScheduleDraft(nextPlatforms = platforms) {
    setScheduleDraft(createInitialScheduleDraft(nextPlatforms));
    setScheduleErrors({});
  }

  function setScheduleField(field, value) {
    setScheduleDraft((current) => ({ ...current, [field]: value }));
  }

  function setScheduleConfigField(field, value) {
    setScheduleDraft((current) => ({
      ...current,
      schedule_config: {
        ...(current.schedule_config || {}),
        [field]: value,
      },
    }));
  }

  function toggleScheduleWeekday(value) {
    setScheduleDraft((current) => {
      const currentValues = current.schedule_config?.weekdays || [];
      const nextValues = currentValues.includes(value)
        ? currentValues.filter((item) => item !== value)
        : [...currentValues, value];
      return {
        ...current,
        schedule_config: {
          ...(current.schedule_config || {}),
          weekdays: nextValues.length ? nextValues : ['1'],
        },
      };
    });
  }

  return {
    scheduleDraft,
    setScheduleDraft,
    resetScheduleDraft,
    setScheduleField,
    setScheduleConfigField,
    toggleScheduleWeekday,
    cronExpression,
    humanSummary,
    scheduleErrors,
    hasScheduleErrors: Object.keys(scheduleErrors).length > 0,
    validateSchedule: () => {
      const nextErrors = validateScheduleDraft(scheduleDraft, tr);
      setScheduleErrors(nextErrors);
      return nextErrors;
    },
  };
}

function validateScheduleDraft(draft, tr) {
  const errors = {};
  if (!String(draft.name || '').trim()) {
    errors.name = tr('schedule_error_name_required');
  }
  const quantity = Number(draft.quantity || 0);
  if (!Number.isFinite(quantity) || quantity < 1 || quantity > 100000) {
    errors.quantity = tr('schedule_error_quantity_invalid');
  }
  const concurrency = Number(draft.concurrency || 0);
  if (!Number.isFinite(concurrency) || concurrency < 1 || concurrency > 64) {
    errors.concurrency = tr('schedule_error_concurrency_invalid');
  }
  if (['daily', 'weekly', 'monthly'].includes(draft.schedule_kind) && !/^\d{2}:\d{2}$/.test(String(draft.time_of_day || ''))) {
    errors.time_of_day = tr('schedule_error_time_required');
  }
  if (draft.schedule_kind === 'interval-minutes') {
    const interval = Number(draft.schedule_config?.interval_minutes || 0);
    if (!Number.isFinite(interval) || interval < 1 || interval > 59) {
      errors.interval_minutes = tr('schedule_error_interval_minutes');
    }
  }
  if (draft.schedule_kind === 'interval-hours') {
    const interval = Number(draft.schedule_config?.interval_hours || 0);
    const minute = Number(draft.schedule_config?.minute ?? -1);
    if (!Number.isFinite(interval) || interval < 1 || interval > 23) {
      errors.interval_hours = tr('schedule_error_interval_hours');
    }
    if (!Number.isFinite(minute) || minute < 0 || minute > 59) {
      errors.minute = tr('schedule_error_minute_offset');
    }
  }
  if (draft.schedule_kind === 'weekly' && !(draft.schedule_config?.weekdays || []).length) {
    errors.weekdays = tr('schedule_error_weekdays_required');
  }
  if (draft.schedule_kind === 'monthly') {
    const day = Number(draft.schedule_config?.day || 0);
    if (!Number.isFinite(day) || day < 1 || day > 31) {
      errors.day = tr('schedule_error_day_invalid');
    }
  }
  return errors;
}

function parseTimestamp(value) {
  if (!value) {
    return null;
  }
  const normalized = String(value).replace(' ', 'T');
  const timestamp = Date.parse(normalized);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function formatDurationMs(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return '0s';
  }
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (minutes) parts.push(`${minutes}m`);
  if (!parts.length || (!days && !hours && seconds)) parts.push(`${seconds}s`);
  return parts.slice(0, 3).join(' ');
}

export function useTaskTiming(task, tr, serverNow = APP_CONFIG.serverNow || null) {
  const serverNowTs = parseTimestamp(serverNow);
  const [clientNow, setClientNow] = useState(() => Date.now());

  useEffect(() => {
    if (!task || !['running', 'stopping', 'queued'].includes(task.status)) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      setClientNow(Date.now());
    }, 1000);
    return () => window.clearInterval(timer);
  }, [task?.id, task?.status]);

  return useMemo(() => {
    if (!task) {
      return {
        durationLabel: tr('task_duration_unknown'),
        startedAtLabel: tr('task_time_unknown'),
        currentRunStartedAtLabel: tr('task_time_unknown'),
        finishedAtLabel: tr('task_time_unknown'),
        timerTone: 'idle',
      };
    }

    const totalStartValue = task.first_started_at || task.started_at || '';
    const currentRunStartValue = task.started_at || task.first_started_at || '';
    const pendingStartValue = (!totalStartValue && task.status === 'queued') ? task.created_at : '';
    const durationStartValue = totalStartValue || pendingStartValue;
    const start = parseTimestamp(durationStartValue);
    const serverOffsetMs = serverNowTs ? (serverNowTs - clientNow) : 0;
    const liveNow = clientNow + serverOffsetMs;
    const end = parseTimestamp(task.finished_at) || liveNow;
    const duration = start ? formatDurationMs(end - start) : tr('task_duration_unknown');
    let durationLabel = tr('task_duration_unknown');
    if (start) {
      if (task.status === 'queued' && !totalStartValue) {
        durationLabel = tr('task_duration_pending', { value: duration });
      } else if (['running', 'stopping'].includes(task.status) || (!task.finished_at && totalStartValue)) {
        durationLabel = tr('task_duration_running', { value: duration });
      } else {
        durationLabel = tr('task_duration_value', { value: duration });
      }
    }
    return {
      durationLabel,
      startedAtLabel: totalStartValue || tr('task_time_unknown'),
      currentRunStartedAtLabel: currentRunStartValue || tr('task_time_unknown'),
      finishedAtLabel: task.finished_at || tr('task_time_unknown'),
      timerTone: ['running', 'stopping'].includes(task.status) ? 'live' : task.status === 'queued' ? 'queued' : 'done',
    };
  }, [clientNow, serverNowTs, task, tr]);
}
