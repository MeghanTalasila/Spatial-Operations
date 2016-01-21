package edu.asu.cse512;

import java.io.Serializable;

public class GeoPointPair implements Serializable{
	private static final long serialVersionUID = -691766005148299237L;
	private GeoPoint p;
	private GeoPoint q;
	private double distance;
	
	GeoPointPair(GeoPoint p, GeoPoint q){
		this.p=p;
		this.q=q;
		this.distance=GeoPoint.getDistance(p, q);
	}
	
	public GeoPoint getP() {
		return p;
	}
	
	public void setP(GeoPoint p) {
		this.p = p;
	}
	
	public GeoPoint getQ() {
		return q;
	}
	
	public void setQ(GeoPoint q) {
		this.q = q;
	}
	
	public double getDistance() {
		return distance;
	}

	@Override
	public String toString() {
		return "GeoPointPair [p=" + p + ", q=" + q + ", distance=" + distance + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(distance);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((p == null) ? 0 : p.hashCode());
		result = prime * result + ((q == null) ? 0 : q.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GeoPointPair other = (GeoPointPair) obj;
		if (Double.doubleToLongBits(distance) != Double.doubleToLongBits(other.distance))
			return false;
		if (p == null) {
			if (other.p != null)
				return false;
		} else if (!p.equals(other.p))
			return false;
		if (q == null) {
			if (other.q != null)
				return false;
		} else if (!q.equals(other.q))
			return false;
		return true;
	}
	
}