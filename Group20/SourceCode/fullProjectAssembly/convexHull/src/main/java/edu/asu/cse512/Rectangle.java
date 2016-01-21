package edu.asu.cse512;

import java.io.Serializable;

public class Rectangle implements Comparable<Rectangle>, Serializable {

	private static final long serialVersionUID = 3231946817586993827L;

	private int id;
	double x1;
	double y1;
	double x2;
	double y2;

	public int getId() {
		return id;
	}

	public double getX1() {
		return x1;
	}

	public double getY1() {
		return y1;
	}

	public double getX2() {
		return x2;
	}

	public double getY2() {
		return y2;
	}

	public int compareTo(Rectangle rectangle) {
		return (this.id - rectangle.id);
	}

	public Rectangle(int id, double x1, double y1, double x2, double y2) {
		super();
		this.id = id;
		this.x1 = Math.min(x1, x2);
		this.y1 = Math.min(y1, y2);
		this.x2 = Math.max(x1, x2);
		this.y2 = Math.max(y1, y2);
	}

	public boolean containsRectangle(Rectangle rectangle) {
		if ((this.x1 <= rectangle.x2 && this.y1 <= rectangle.y2 && this.x1 >= rectangle.x1 && this.y1 >= rectangle.y1)
				&& (this.x2 <= rectangle.x2 && this.y2 <= rectangle.y2 && this.x2 >= rectangle.x1
						&& this.y2 >= rectangle.y1)) {
			return true;
		}
		return false;
	}

	public Rectangle(double x1, double y1, double x2, double y2) {
		this.x1 = x1;
		this.x2 = x2;
		this.y1 = y1;
		this.y2 = y2;
	}
	public boolean containsPoints(GeoPoint point) {

		double x = point.getX();
		double y = point.getY();

		return this.x1 <= x && this.y1 <= y && this.x2 >= x && this.y2 >= y;
	}

}
