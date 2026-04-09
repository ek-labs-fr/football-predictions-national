import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface Team {
  id: number;
  name: string;
  country: string | null;
  national: boolean;
  logo: string | null;
  fifa_rank: number | null;
}

export interface TeamListResponse {
  teams: Team[];
  total: number;
}

@Injectable({ providedIn: 'root' })
export class TeamService {
  private readonly apiUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  getTeams(): Observable<TeamListResponse> {
    return this.http.get<TeamListResponse>(`${this.apiUrl}/teams`);
  }

  getTeam(id: number): Observable<Team> {
    return this.http.get<Team>(`${this.apiUrl}/teams/${id}`);
  }
}
