/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.exception;

public class PhysicalTaskExecuteFailureException extends PhysicalException {

  private static final long serialVersionUID = 1535411462205865453L;

  public PhysicalTaskExecuteFailureException(String message) {
    super(message);
  }

  public PhysicalTaskExecuteFailureException(String message, Throwable cause) {
    super(message, cause);
  }
}
