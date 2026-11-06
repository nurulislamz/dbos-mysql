/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'index',
    {
      type: 'category',
      label: 'Get started',
      items: ['quickstart', 'integrate'],
    },
    'architecture',
    {
      type: 'category',
      label: 'Programming guide',
      items: [
        'guide/workflows',
        'guide/steps',
        'guide/queues',
        'guide/communication',
        'guide/time',
        'guide/management',
        'guide/pydantic-ai',
      ],
    },
    {
      type: 'category',
      label: 'Temporal patterns',
      link: {type: 'doc', id: 'patterns/index'},
      items: [
        section('Task orchestration', 'patterns/task-orchestration', [
          'patterns/child-workflows',
          'patterns/parallel-execution',
          'patterns/pick-first',
        ]),
        section('Messaging', 'patterns/messaging', [
          'patterns/signal-with-start',
          'patterns/request-response',
          'patterns/event-accumulator',
        ]),
        section('Entity and lifecycle', 'patterns/entity', [
          'patterns/entity-workflow',
          'patterns/continue-as-new',
          'patterns/updatable-timer',
        ]),
        section('External interaction', 'patterns/external', [
          'patterns/polling',
          'patterns/long-running-step',
          'patterns/delayed-start',
          'patterns/delayed-callback',
          'patterns/approval',
        ]),
        section('Distributed transactions', 'patterns/transactions', [
          'patterns/saga',
          'patterns/early-return',
        ]),
        section('Retries', 'patterns/retries', [
          'patterns/fixed-retries',
          'patterns/wall-time-retries',
          'patterns/non-retryable',
          'patterns/delayed-retry',
          'patterns/fast-slow-retries',
          'patterns/retry-alerting',
          'patterns/resumable',
        ]),
        section('Batch processing', 'patterns/batch', [
          'patterns/fan-out',
          'patterns/batch-iterator',
          'patterns/sliding-window',
          'patterns/mapreduce',
        ]),
        section('Throughput', 'patterns/throughput', [
          'patterns/rate-limiting',
          'patterns/priority',
          'patterns/fairness',
        ]),
        section('Latency', 'patterns/latency', [
          'patterns/local-steps',
          'patterns/early-return-local',
          'patterns/eager-start',
        ]),
        section('Workers', 'patterns/workers', [
          'patterns/worker-queues',
          'patterns/dependency-injection',
        ]),
      ],
    },
    'production',
    'reference',
    'coming-soon',
  ],
};

function section(label, id, items) {
  return {
    type: 'category',
    label,
    link: {type: 'doc', id},
    items,
  };
}

module.exports = sidebars;
