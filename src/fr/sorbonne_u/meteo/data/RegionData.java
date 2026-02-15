package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 */
public class RegionData implements RegionI {

	private static final long serialVersionUID = 4505980791444655950L;
	private double xMin;
	private double xMax;
	private double yMin;
	private double yMax;
	
	public RegionData(double xMin, double xMax, double yMin, double yMax) {
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
	}
	
	public double[] getLimits() {
		return new double[]{xMin, xMax, yMin, yMax};
	}
	
	@Override
	public boolean in(PositionI p) {
		PositionData pos = (PositionData) p;
		
		if(pos.getPosX() <= this.xMax && pos.getPosX() >= this.xMin && pos.getPosY() <= this.yMax
				&& pos.getPosY() >= this.yMin) {
			return true;
		}
		
		
		return false;
	
	}

}