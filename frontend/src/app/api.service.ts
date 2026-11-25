import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

export type Priority = 'low' | 'normal' | 'high';

export interface Job {
  id: string;
  team: string;
  gpus: number;
  estimated_minutes: number;
  priority: Priority;
  effective_priority: number | null;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  submitted_at: string;
  started_at: string | null;
  ended_at: string | null;
  node_id: string | null;
  wait_minutes: number | null;
  attempts: number;
  reason: string | null;
  first_started_at: string | null;
  interruptions: number;
  runs_on_interruptible: boolean | null;
}

export interface BrokerNode {
  id: string;
  type: string;
  gpus: number;
  used_gpus: number;
  price_per_hour: number;
  state: 'booting' | 'ready' | 'terminated' | 'failed';
  team: string;
  job_ids: string[];
  launched_at: string;
  ready_at: string | null;
  idle_since: string | null;
  draining: boolean;
  retiring: boolean;
  interruptible: boolean;
  reserved_for_high: boolean;
  reclaim_notice: boolean;
  teams: string[];
  cost_so_far: number;
}

export interface TeamCost {
  team: string;
  gpu_hours: number;
  busy_cost: number;
  idle_cost: number;
  total_cost: number;
  idle_share: number;
  jobs: number;
}

export interface CostReport {
  start: string;
  end: string;
  teams: TeamCost[];
  total_cost: number;
  total_idle_cost: number;
  total_gpu_hours: number;
  idle_pool_breakdown: Record<string, number>;
  provider_invoice_to_date: number;
}

export interface Summary {
  now: string;
  queued: number;
  running: number;
  nodes_alive: number;
  max_nodes: number;
  gpus_total: number;
  gpus_used: number;
  backoff_until: Record<string, string>;
  stats: Record<string, number>;
  config: Record<string, unknown>;
}

export interface SubmitRequest {
  team: string;
  gpus: number;
  estimated_minutes: number;
  priority: Priority;
  simulated_runtime_minutes?: number | null;
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private base = '/api';

  summary(): Observable<Summary> {
    return this.http.get<Summary>(`${this.base}/summary`);
  }
  queue(): Observable<Job[]> {
    return this.http.get<Job[]>(`${this.base}/queue`);
  }
  jobs(limit = 50): Observable<Job[]> {
    return this.http.get<Job[]>(`${this.base}/jobs`, { params: { limit } });
  }
  nodes(): Observable<BrokerNode[]> {
    return this.http.get<BrokerNode[]>(`${this.base}/nodes`);
  }
  cost(start?: string, end?: string): Observable<CostReport> {
    let params = new HttpParams();
    if (start) params = params.set('start', start);
    if (end) params = params.set('end', end);
    return this.http.get<CostReport>(`${this.base}/cost`, { params });
  }
  submit(req: SubmitRequest, idempotencyKey: string): Observable<Job> {
    const headers = new HttpHeaders({ 'Idempotency-Key': idempotencyKey });
    return this.http.post<Job>(`${this.base}/jobs`, req, { headers });
  }
  cancel(id: string): Observable<Job> {
    return this.http.post<Job>(`${this.base}/jobs/${id}/cancel`, {});
  }
  advance(minutes: number): Observable<{ now: string }> {
    return this.http.post<{ now: string }>(`${this.base}/clock/advance`, { minutes });
  }
}
