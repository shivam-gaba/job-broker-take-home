import { Component, DestroyRef, OnInit, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DatePipe, DecimalPipe, JsonPipe, KeyValuePipe, PercentPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpErrorResponse } from '@angular/common/http';
import { Subject, forkJoin, interval, startWith, switchMap, catchError, of, merge } from 'rxjs';
import { ApiService, BrokerNode, CostReport, Job, Priority, SubmitRequest, Summary } from './api.service';

const PRIORITY_LABEL = ['low', 'normal', 'high'];

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [FormsModule, DatePipe, DecimalPipe, PercentPipe, KeyValuePipe, JsonPipe],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent implements OnInit {
  private api = inject(ApiService);
  private destroyRef = inject(DestroyRef);
  private refresh$ = new Subject<void>();

  summary = signal<Summary | null>(null);
  queue = signal<Job[]>([]);
  jobs = signal<Job[]>([]);
  nodes = signal<BrokerNode[]>([]);
  cost = signal<CostReport | null>(null);
  error = signal<string | null>(null);
  message = signal<string | null>(null);
  busy = signal(false);

  // cost window (datetime-local strings, interpreted as UTC)
  costStart = '';
  costEnd = '';

  form: SubmitRequest = { team: 'vision', gpus: 1, estimated_minutes: 30, priority: 'normal', simulated_runtime_minutes: null };
  readonly gpuOptions = [1, 2, 4, 8];
  readonly priorities: Priority[] = ['low', 'normal', 'high'];

  recentJobs = computed(() => this.jobs().filter((j) => j.status !== 'queued').slice(0, 25));
  teamsInCost = computed(() => (this.cost()?.teams ?? []).filter((t) => t.total_cost > 0 || t.gpu_hours > 0));

  ngOnInit(): void {
    merge(interval(2000).pipe(startWith(0)), this.refresh$)
      .pipe(
        switchMap(() =>
          forkJoin({
            summary: this.api.summary(),
            queue: this.api.queue(),
            jobs: this.api.jobs(100),
            nodes: this.api.nodes(),
            cost: this.api.cost(this.toIso(this.costStart), this.toIso(this.costEnd)),
          }).pipe(
            catchError((e: HttpErrorResponse) => {
              this.error.set(`Cannot reach the broker API (${e.status || 'network error'}). Is the backend running on :8000?`);
              return of(null);
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((data) => {
        if (!data) return;
        this.error.set(null);
        this.summary.set(data.summary);
        this.queue.set(data.queue);
        this.jobs.set(data.jobs);
        this.nodes.set(data.nodes);
        this.cost.set(data.cost);
      });
  }

  refresh(): void {
    this.refresh$.next();
  }

  submit(): void {
    const req: SubmitRequest = { ...this.form, team: this.form.team.trim() };
    if (!req.simulated_runtime_minutes) delete req.simulated_runtime_minutes;
    this.busy.set(true);
    // one key per click: a retried request (double click, flaky network) is not a second job
    this.api.submit(req, crypto.randomUUID()).subscribe({
      next: (job) => {
        this.flash(`Submitted ${job.id} (${job.status})`);
        this.busy.set(false);
        this.refresh();
      },
      error: (e: HttpErrorResponse) => {
        this.busy.set(false);
        this.flash(`Submit failed: ${this.detail(e)}`);
      },
    });
  }

  cancel(job: Job): void {
    this.api.cancel(job.id).subscribe({
      next: () => {
        this.flash(`Cancelled ${job.id}`);
        this.refresh();
      },
      error: (e: HttpErrorResponse) => this.flash(`Cancel failed: ${this.detail(e)}`),
    });
  }

  advance(minutes: number): void {
    this.busy.set(true);
    this.api.advance(minutes).subscribe({
      next: () => {
        this.busy.set(false);
        this.refresh();
      },
      error: (e: HttpErrorResponse) => {
        this.busy.set(false);
        this.flash(`Advance failed: ${this.detail(e)}`);
      },
    });
  }

  applyCostWindow(): void {
    this.refresh();
  }

  clearCostWindow(): void {
    this.costStart = '';
    this.costEnd = '';
    this.refresh();
  }

  priorityLabel(p: number | null): string {
    return p === null ? '' : PRIORITY_LABEL[p];
  }

  nodeStatus(n: BrokerNode): string {
    if (n.state === 'booting') return 'booting';
    if (n.retiring) return 'retiring';
    if (n.draining) return 'draining';
    return n.job_ids.length ? 'busy' : 'idle';
  }

  gpuSlots(n: BrokerNode): boolean[] {
    return Array.from({ length: n.gpus }, (_, i) => i < n.used_gpus);
  }

  private toIso(local: string): string | undefined {
    return local ? `${local}:00Z` : undefined;
  }

  private detail(e: HttpErrorResponse): string {
    const d = e.error?.detail;
    if (typeof d === 'string') return d;
    if (Array.isArray(d)) return d.map((x: { msg: string }) => x.msg).join('; ');
    return e.message;
  }

  private flash(text: string): void {
    this.message.set(text);
    setTimeout(() => this.message() === text && this.message.set(null), 4000);
  }
}
