package uconn.stormcplex.salr.mp;

import java.io.Serializable;

public class Line implements Serializable {
	public int i;
	public int j;
	public double g;
	public double b;
	public double constraint;

	public Line(int i, int j, double g, double b, double constraint) {
		this.i = i;
		this.j = j;
		this.g = g;
		this.b = b;
		this.constraint = constraint;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i;
		result = prime * result + j;
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
		Line other = (Line) obj;
		if (i != other.i)
			return false;
		if (j != other.j)
			return false;
		return true;
	}
}
