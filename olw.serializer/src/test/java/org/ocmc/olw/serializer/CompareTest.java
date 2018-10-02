package org.ocmc.olw.serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CompareTest {
	
	
	List<String> l1 = new ArrayList<String>();
	List<String> l2 = new ArrayList<String>();

	public CompareTest() {
		l1.add("apple");
		l1.add("orange");
		l2.addAll(l1);
		l2.add("peach");
		Collections.sort(l1);
		Collections.sort(l2);
	}
	
	private int firstDif() {
		int i = 0;
		while (this.l1.get(i).equals(l2.get(i))) {
			i++;
		}
		return i;
	}
	
	public static void main(String[] args) {
		List<String> l1 = new ArrayList<String>();
		List<String> l2 = new ArrayList<String>();
		
		l1.add("apple");
		l1.add("orange");
		
		l2.addAll(l1);
		l2.add("peach");
		
		l1.add("grape");

		Collections.sort(l1);
		Collections.sort(l2);
		
		System.out.println(l1);
		System.out.println(l2);
		
		int size1 = l1.size();
		int size2 = l2.size();
		int i1 = 0;
		int i2 = 0;
		
		while (i1 < size1 || i2 < size2) {
		    String s1 = "";
			String s2 = "";
			if (i1 < size1) {
			    s1 = l1.get(i1);
			}
			if (i2 < size2) {
				s2 = l2.get(i2);
			}
			System.out.println(s1 + " ? " + s2);
			if (s1.length() == 0) {
				System.out.println("l1 missing " + s2);
				i2++;
			} else if (s2.length() == 0) {
				System.out.println("l2 missing " + s1);
				i1++;
			} else {
				int compResult = s1.compareTo(s2);
				if (compResult < 0) {
					System.out.println("l2 missing " + s1);
					i1++;
				} else if (compResult == 0) {
					i1++;
					i2++;
				} else {
					System.out.println("l1 missing " + s2);
					i2++;
				}
			}
		}
	}
}
