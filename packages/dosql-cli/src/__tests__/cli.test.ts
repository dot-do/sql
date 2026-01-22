import { describe, it, expect } from 'vitest';
import { createCLI } from '../index.js';

describe('dosql CLI', () => {
  it('should create CLI program', () => {
    const program = createCLI();
    expect(program.name()).toBe('dosql');
  });

  it('should have init command', () => {
    const program = createCLI();
    const initCommand = program.commands.find(cmd => cmd.name() === 'init');
    expect(initCommand).toBeDefined();
  });

  it('should have migrate command', () => {
    const program = createCLI();
    const migrateCommand = program.commands.find(cmd => cmd.name() === 'migrate');
    expect(migrateCommand).toBeDefined();
  });

  it('should have generate command', () => {
    const program = createCLI();
    const generateCommand = program.commands.find(cmd => cmd.name() === 'generate');
    expect(generateCommand).toBeDefined();
  });

  it('should have proper version', () => {
    const program = createCLI();
    expect(program.version()).toBeDefined();
  });

  it('should have proper description', () => {
    const program = createCLI();
    expect(program.description()).toContain('DoSQL');
  });
});
